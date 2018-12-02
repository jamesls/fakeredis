import time
import itertools
import operator

import hypothesis
from hypothesis.stateful import rule
import hypothesis.strategies as st
from nose.tools import assert_equal

import redis
import fakeredis


@hypothesis.settings(max_examples=1000, timeout=hypothesis.unlimited)
class HypothesisStrictRedis(hypothesis.stateful.RuleBasedStateMachine):
    def __init__(self):
        super(HypothesisStrictRedis, self).__init__()
        self.fake = fakeredis.FakeStrictRedis()
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
            # TODO reenable after implementing tools to control error check ordering
            #assert_equal(fake_exc.args, real_exc.args)
        elif fake_exc is not None:
            raise fake_exc
        else:
            assert_equal(fake_result, real_result)

    keys = hypothesis.stateful.Bundle('keys')
    fields = hypothesis.stateful.Bundle('fields')
    values = hypothesis.stateful.Bundle('values')
    scores = hypothesis.stateful.Bundle('scores')

    int_as_bytes = st.builds(lambda x: str(x).encode(), st.integers())
    float_as_bytes = st.builds(lambda x: repr(x).encode(), st.floats(width=32))
    score_tests = scores | st.builds(lambda x: b'(' + repr(x).encode(), scores)
    string_tests = (
        st.sampled_from([b'+', b'-'])
        | st.builds(operator.add, st.sampled_from([b'(', b'[']), fields))

    @rule(target=keys, key=st.binary())
    def make_key(self, key):
        return key

    @rule(target=fields, field=st.binary())
    def make_field(self, field):
        return field

    @rule(target=values, value=st.binary() | int_as_bytes | float_as_bytes)
    def make_value(self, value):
        return value

    @rule(target=scores, value=st.floats(width=32))
    def make_score(self, value):
        return value

    # Connection commands
    # TODO: tests for select, swapdb

    @rule(value=values)
    def echo(self, value):
        self._compare('echo', value)

    @rule(args=st.lists(values, max_size=2))
    def ping(self, args):
        self._compare('execute_command', b'ping', *args)

    # Key commands
    # TODO: add special testing for
    # - expiry-related commands
    # - move
    # - randomkey

    @rule(key=keys)
    def delete(self, key):
        self._compare('delete', key)

    @rule(key=keys)
    def exists(self, key):
        self._compare('exists', key)

    @rule(pattern=st.none() | st.binary())
    def keys_(self, pattern):
        self._compare('keys', pattern)

    @rule(key=keys)
    def persist(self, key):
        self._compare('persist', key)

    @rule(key=keys, newkey=keys)
    def rename(self, key, newkey):
        self._compare('rename', key, newkey)

    @rule(key=keys, newkey=keys)
    def renamenx(self, key, newkey):
        self._compare('renamenx', key, newkey)

    # List commands
    # TODO: blocking commands

    @rule(key=keys, index=st.integers())
    def lindex(self, key, index):
        self._compare('lindex', key, index)

    @rule(key=keys, where=st.sampled_from(['before', 'after', 'BEFORE', 'AFTER']) | st.binary(),
          pivot=values, value=values)
    def linsert(self, key, where, pivot, value):
        self._compare('linsert', key, where, pivot, value)

    @rule(key=keys)
    def llen(self, key):
        self._compare('llen', key)

    @rule(key=keys)
    def lpop(self, key):
        self._compare('lpop', key)

    @rule(key=keys, values=st.lists(values))
    def lpush(self, key, values):
        self._compare('lpush', key, *values)

    @rule(key=keys, values=st.lists(values))
    def lpushx(self, key, values):
        self._compare('lpushx', key, *values)

    @rule(key=keys, start=st.integers(), stop=st.integers())
    def lrange(self, key, start, stop):
        self._compare('llrange', key, start, stop)

    @rule(key=keys, count=st.integers(), value=values)
    def lrem(self, key, count, value):
        self._compare('lrem', key, count, value)

    @rule(key=keys, index=st.integers(), value=values)
    def lset(self, key, index, value):
        self._compare('lset', key, index, value)

    @rule(key=keys, start=st.integers(), stop=st.integers())
    def ltrim(self, key, start, stop):
        self._compare('ltrim', key, start, stop)

    @rule(key=keys)
    def rpop(self, key):
        self._compare('rpop', key)

    @rule(src=keys, dst=keys)
    def rpoplpush(self, src, dst):
        self._compare('rpoplpush', src, dst)

    @rule(key=keys, values=st.lists(values))
    def rpush(self, key, values):
        self._compare('rpush', key, *values)

    @rule(key=keys, values=st.lists(values))
    def rpushx(self, key, values):
        self._compare('rpushx', key, *values)

    # Sorted set commands

    @rule(key=keys, items=st.lists(st.tuples(scores, fields)))
    def zadd(self, key, items):
        # TODO: test xx, nx, ch, incr
        # TODO: support redis-py 3
        print(key, items)
        flat_items = itertools.chain(*items)
        self._compare('zadd', *flat_items)

    @rule(key=keys)
    def zcard(self, key):
        self._compare('zcard', key)

    @rule(key=keys, min=score_tests, max=score_tests)
    def zcount(self, key, min, max):
        self._compare('zcount', key, min, max)

    @rule(key=keys, increment=scores, member=fields)
    def zincrby(self, key, increment, member):
        self._compare('zincrby', key, increment, member)

    @rule(key=keys, min=string_tests, max=string_tests)
    def zlexcount(self, key, min, max):
        self._compare('zlexcount', key, min, max)

    @rule(key=keys, start=st.integers(), stop=st.integers(), withscores=st.booleans())
    def zrange(self, key, start, stop, withscores):
        self._compare('zrange', key, start, stop, withscores)

    @rule(key=keys, start=st.integers(), stop=st.integers(), withscores=st.booleans())
    def zrevrange(self, key, start, stop, withscores):
        self._compare('zrevrange', key, start, stop, withscores)

    @rule(key=keys, min=string_tests, max=string_tests,
          start=st.none() | st.integers(), count=st.none() | st.integers())
    def zrangebylex(self, key, min, max, start, count):
        self._compare('zrangebylex', min, max, start, count)

    @rule(key=keys, min=string_tests, max=string_tests,
          start=st.none() | st.integers(), count=st.none() | st.integers())
    def zrevrangebylex(self, key, min, max, start, count):
        self._compare('zrevrangebylex', min, max, start, count)

    # Transaction commands
    # TODO: create separate test case for transactions. They're not a good
    # fit for this state machine since they move return values later.

    @rule()
    def multi(self):
        self._compare('multi')

    @rule()
    def discard(self):
        self._compare('discard')

    @rule()
    def exec(self):
        self._compare('exec')

    @rule(key=keys)
    def watch(self, key):
        self._compare('watch', key)

    @rule()
    def unwatch(self):
        self._compare('unwatch')

    # String commands

    @rule(key=keys, value=values)
    def append(self, key, value):
        self._compare('append', key, value)

    @rule(key=keys,
          start=st.none() | values,
          end=st.none() | values)
    def bitcount(self, key, start, end):
        self._compare('bitcount', key, start, end)

    @rule(key=keys, amount=st.none() | values)
    def decrby(self, key, amount):
        if amount is None:
            self._compare('decrby', key)
        else:
            self._compare('decrby', key, amount=amount)

    @rule(key=keys, amount=st.none() | values)
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

    @rule(items=st.lists(st.tuples(keys, values)))
    def mset(self, items):
        args = itertools.chain(*items)
        self._compare('mset', *args)

    @rule(items=st.lists(st.tuples(keys, values)))
    def msetnx(self, items):
        args = itertools.chain(*items)
        self._compare('mset', *args)

    @rule(key=keys, value=values, nx=st.booleans(), xx=st.booleans())
    def set(self, key, value, nx, xx):
        self._compare('set', key, value, nx=nx, xx=xx)

    @rule(key=keys, value=values, seconds=st.integers(min_value=1000000000))
    def setex(self, key, seconds, value):
        self._compare('setex', key, seconds, value)

    @rule(key=keys, value=values, ms=st.integers(min_value=1000000000000))
    def psetex(self, key, ms, value):
        self._compare('psetex', key, ms, value)

    @rule(key=keys, value=values)
    def setnx(self, key, value):
        self._compare('setnx', key, value)

    @rule(key=keys, offset=st.integers(), value=values)
    def setrange(self, key, offset, value):
        self._compare('setrange', key, offset, value)

    @rule(key=keys)
    def strlen(self, key):
        self._compare('strlen', key)

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
