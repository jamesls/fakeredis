import time

import hypothesis
from hypothesis.stateful import rule
import hypothesis.strategies as st
from nose.tools import assert_equal

import redis
import fakeredis2


@hypothesis.settings(max_examples=1000, timeout=hypothesis.unlimited)
class HypothesisStrictRedis(hypothesis.stateful.RuleBasedStateMachine):
    def __init__(self):
        super(HypothesisStrictRedis, self).__init__()
        self.fake = fakeredis2.FakeStrictRedis()
        self.real = redis.StrictRedis('localhost', port=6379)
        self.real.flushall()

    def teardown(self):
        self.real.connection_pool.disconnect()
        self.fake.connection_pool.disconnect()
        super(HypothesisStrictRedis, self).teardown()

    def _compare(self, cmd, *args, **kwargs):
        fake_exc = None
        real_exc = None

        try:
            fake_result = getattr(self.fake, cmd)(*args, **kwargs)
        except Exception as exc:
            fake_exc = exc

        try:
            real_result = getattr(self.real, cmd)(*args, **kwargs)
        except Exception as exc:
            real_exc = exc

        if real_exc is not None:
            assert_equal(type(fake_exc), type(real_exc))
            assert_equal(fake_exc.args, real_exc.args)
        elif fake_exc is not None:
            raise fake_exc
        else:
            assert_equal(fake_result, real_result)

    keys = hypothesis.stateful.Bundle('keys')
    fields = hypothesis.stateful.Bundle('fields')
    values = hypothesis.stateful.Bundle('values')

    int_as_bytes = st.builds(lambda x: str(x).encode(), st.integers())
    float_as_bytes = st.builds(lambda x: repr(x).encode(), st.floats(width=32))

    @rule(target=keys, key=st.binary())
    def make_key(self, key):
        return key

    @rule(target=fields, field=st.binary())
    def make_field(self, field):
        return field

    @rule(target=values, value=st.binary() | int_as_bytes | float_as_bytes)
    def make_value(self, value):
        return value

    # Connection commands
    # TODO: tests for select, swapdb

    @rule(value=st.binary())
    def echo(self, value):
        self._compare('echo', value)

    @rule(args=st.lists(st.binary(), max_size=2))
    def ping(self, args):
        self._compare('execute_command', b'ping', *args)

    # Key commands
    # TODO: add special testing for expiry-related commands

    @rule(key=keys)
    def delete(self, key):
        self._compare('delete', key)

    @rule(key=keys)
    def exists(self, key):
        self._compare('exists', key)

    @rule(pattern=st.none() | st.binary())
    def keys_(self, pattern):
        self._compare('keys', pattern)

    # String commands

    @rule(key=keys, value=values)
    def append(self, key, value):
        self._compare('append', key, value)

    @rule(key=keys,
          start=st.none() | st.integers() | st.binary(),
          end=st.none() | st.integers() | st.binary())
    def bitcount(self, key, start, end):
        self._compare('bitcount', key, start, end)

    @rule(key=keys, amount=st.none() | st.integers() | st.binary())
    def decrby(self, key, amount):
        if amount is None:
            self._compare('decrby', key)
        else:
            self._compare('decrby', key, amount=amount)

    @rule(key=keys, amount=st.none() | st.integers() | st.binary())
    def incrby(self, key, amount):
        if amount is None:
            self._compare('incrby', key)
        else:
            self._compare('incrby', key, amount=amount)

    # Disabled for now because Python can't exactly model the long doubles.
    # TODO: make a more targeted test that checks the basics.
    # @rule(key=keys, amount=st.floats(width=32))
    # def incrbyfloat(self, key, amount):
    #     self._compare('incrbyfloat', key, amount)
    #     # Check how it gets stringified, without relying on hypothesis
    #     # to get generate a get call before it gets overwritten.
    #     self._compare('get', key)

    @rule(key=keys)
    def get(self, key):
        self._compare('get', key)

    @rule(key=keys, offset=st.integers())
    def getbit(self, key, offset):
        self._compare('getbit', key, offset)

    @rule(key=keys, start=st.integers(), end=st.integers())
    def getrange(self, key, start, end):
        self._compare('getrange', key, start, end)

    @rule(key=keys, start=st.integers(), end=st.integers())
    def substr(self, key, start, end):
        self._compare('substr', key, start, end)

    @rule(key=keys, value=values)
    def getset(self, key, value):
        self._compare('getset', key, value)

    @rule(keys=st.lists(keys))
    def mget(self, keys):
        self._compare('mget', *keys)

    @rule(key=keys, value=values, nx=st.booleans(), xx=st.booleans())
    def set(self, key, value, nx, xx):
        self._compare('set', key, value, nx=nx, xx=xx)

    # Hash commands

    @rule(key=keys, field=st.lists(fields))
    def hdel(self, key, field):
        self._compare('hget', key, *field)

    @rule(key=keys, field=fields)
    def hget(self, key, field):
        self._compare('hget', key, field)

    @rule(key=keys, field=fields, value=values)
    def hset(self, key, field, value):
        self._compare('hset', key, field, value)

    # Server commands

    @rule(asynchronous=st.booleans())
    def flushdb(self, asynchronous):
        self._compare('flushdb', asynchronous=asynchronous)

    @rule(asynchronous=st.booleans())
    def flushall(self, asynchronous):
        self._compare('flushall', asynchronous=asynchronous)

    # Other tests

    @rule(command=st.text(), args=st.lists(st.binary() | st.text()))
    def bad_command(self, command, args):
        # redis-py splits the command on spaces, and hangs if that ends up
        # being an empty list
        st.assume(command.split())
        self._compare('execute_command', command, *args)


TestHypothesisStrictRedis = HypothesisStrictRedis.TestCase
