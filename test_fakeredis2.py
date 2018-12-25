import time
import itertools
import operator
import sys
import unittest

import hypothesis
from hypothesis.stateful import rule
import hypothesis.strategies as st
from nose.tools import assert_equal

import redis
import fakeredis


int_as_bytes = st.builds(lambda x: str(x).encode(), st.integers())
float_as_bytes = st.builds(lambda x: repr(x).encode(), st.floats(width=32))
counts = st.integers(min_value=-3, max_value=3) | st.integers()
# The filter is to work around https://github.com/antirez/redis/issues/5632
patterns = (st.text(alphabet=st.sampled_from('[]^$*.?-azAZ\\\r\n\t'))
            | st.binary().filter(lambda x: b'\0' not in x))


class WrappedException(object):
    """Wraps an exception for the purposes of comparison."""
    def __init__(self, exc):
        self.wrapped = exc

    def __str__(self):
        return str(self.wrapped)

    def __repr__(self):
        return 'WrappedException({0!r})'.format(self.wrapped)

    def __eq__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        if type(self.wrapped) != type(other.wrapped):
            return False
        # TODO: re-enable after more carefully handling order of error checks
        # return self.wrapped.args == other.wrapped.args
        return True

    def __ne__(self, other):
        if not isinstance(other, WrappedException):
            return NotImplemented
        return not self == other


def wrap_exceptions(obj):
    if isinstance(obj, list):
        return [wrap_exceptions(item) for item in obj]
    elif isinstance(obj, Exception):
        return WrappedException(obj)
    else:
        return obj


def sort_list(lst):
    if isinstance(lst, list):
        return sorted(lst)
    else:
        return lst


@hypothesis.settings(max_examples=1000, timeout=hypothesis.unlimited)
class BaseMachine(hypothesis.stateful.RuleBasedStateMachine):
    def __init__(self):
        super(BaseMachine, self).__init__()
        self.fake = fakeredis.FakeStrictRedis()
        self.real = redis.StrictRedis('localhost', port=6379)
        try:
            self.real.execute_command('discard')
        except redis.ResponseError:
            pass
        self.real.flushall()

    def teardown(self):
        self.real.connection_pool.disconnect()
        self.fake.connection_pool.disconnect()
        super(BaseMachine, self).teardown()

    def _evaluate(self, client, cmd, *args, **kwargs):
        normalize = kwargs.pop('normalize', lambda x: x)
        try:
            result = normalize(client.execute_command(cmd, *args))
            exc = None
        except Exception as e:
            result = exc = e
        return wrap_exceptions(result), exc

    def _compare(self, cmd, *args, **kwargs):
        fake_result, fake_exc = self._evaluate(self.fake, cmd, *args, **kwargs)
        real_result, real_exc = self._evaluate(self.real, cmd, *args, **kwargs)

        if fake_exc is not None and real_exc is None:
            raise fake_exc
        elif real_exc is not None and fake_exc is None:
            assert_equal(real_exc, fake_exc, "Expected exception {0} not raised".format(real_exc))
        else:
            assert_equal(fake_result, real_result)

    keys = hypothesis.stateful.Bundle('keys')
    fields = hypothesis.stateful.Bundle('fields')
    values = hypothesis.stateful.Bundle('values')
    scores = hypothesis.stateful.Bundle('scores')

    @rule(target=keys, key=st.binary())
    def make_key(self, key):
        return key

    @rule(target=fields, field=st.binary())
    def make_field(self, field):
        return field

    @rule(target=values, value=st.binary() | int_as_bytes | float_as_bytes)
    def make_value(self, value):
        return value

    # Key commands
    # TODO: add special testing for
    # - expiry-related commands
    # - move
    # - randomkey

    @rule(key=st.lists(keys))
    def delete(self, key):
        self._compare('delete', *key)

    @rule(key=keys)
    def exists(self, key):
        self._compare('exists', key)

    # Disabled for now due to redis giving wrong answers
    # (https://github.com/antirez/redis/issues/5632)
    # @rule(pattern=st.none() | patterns)
    # def keys_(self, pattern):
    #     self._compare('keys', pattern)

    @rule(key=keys)
    def persist(self, key):
        self._compare('persist', key)

    @rule(key=keys, newkey=keys)
    def rename(self, key, newkey):
        self._compare('rename', key, newkey)

    @rule(key=keys, newkey=keys)
    def renamenx(self, key, newkey):
        self._compare('renamenx', key, newkey)

    @rule(key=keys)
    def type(self, key):
        self._compare('type', key)


class ConnectionMachine(BaseMachine):
    # TODO: tests for select, swapdb
    values = BaseMachine.values

    @rule(value=values)
    def echo(self, value):
        self._compare('echo', value)

    @rule(args=st.lists(values, max_size=2))
    def ping(self, args):
        self._compare('ping', *args)


TestConnection = ConnectionMachine.TestCase


class StringMachine(BaseMachine):
    keys = BaseMachine.keys
    values = BaseMachine.values

    @rule(key=keys, value=values)
    def append(self, key, value):
        self._compare('append', key, value)

    @rule(key=keys,
          start=st.none() | values,
          end=st.none() | values)
    def bitcount(self, key, start, end):
        self._compare('bitcount', key, start, end)

    @rule(key=keys, amount=st.none() | values)
    def decr(self, key, amount):
        if amount is None:
            self._compare('decr', key)
        else:
            self._compare('decrby', key, amount)

    @rule(key=keys, amount=st.none() | values)
    def incr(self, key, amount):
        if amount is None:
            self._compare('incr', key)
        else:
            self._compare('incrby', key, amount)

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

    @rule(key=keys, offset=counts)
    def getbit(self, key, offset):
        self._compare('getbit', key, offset)

    @rule(key=keys, offset=counts, value=st.integers(min_value=0, max_value=1) | st.integers())
    def setbit(self, key, offset, value):
        self._compare('setbit', key, offset, value)

    @rule(key=keys, start=counts, end=counts)
    def getrange(self, key, start, end):
        self._compare('getrange', key, start, end)

    @rule(key=keys, start=counts, end=counts)
    def substr(self, key, start, end):
        self._compare('substr', key, start, end)

    @rule(key=keys, value=values)
    def getset(self, key, value):
        self._compare('getset', key, value)

    @rule(keys=st.lists(keys))
    def mget(self, keys):
        self._compare('mget', *keys)

    @rule(items=st.dictionaries(keys, values))
    def mset(self, items):
        self._compare('mset', items)

    @rule(items=st.dictionaries(keys, values))
    def msetnx(self, items):
        self._compare('msetnx', items)

    @rule(key=keys, value=values, nx=st.booleans(), xx=st.booleans())
    def set(self, key, value, nx, xx):
        args = ['set', key, value]
        if nx:
            args.append('nx')
        if xx:
            args.append('xx')
        self._compare(*args)

    @rule(key=keys, value=values, seconds=st.integers(min_value=1000000000))
    def setex(self, key, seconds, value):
        self._compare('setex', key, seconds, value)

    @rule(key=keys, value=values, ms=st.integers(min_value=1000000000000))
    def psetex(self, key, ms, value):
        self._compare('psetex', key, ms, value)

    @rule(key=keys, value=values)
    def setnx(self, key, value):
        self._compare('setnx', key, value)

    @rule(key=keys, offset=counts, value=values)
    def setrange(self, key, offset, value):
        self._compare('setrange', key, offset, value)

    @rule(key=keys)
    def strlen(self, key):
        self._compare('strlen', key)


TestString = StringMachine.TestCase


class HashMachine(BaseMachine):
    keys = BaseMachine.keys
    values = BaseMachine.values
    fields = BaseMachine.fields

    @rule(key=keys, field=st.lists(fields))
    def hdel(self, key, field):
        self._compare('hdel', key, *field)

    @rule(key=keys, field=fields)
    def hexists(self, key, field):
        self._compare('hexists', key, field)

    @rule(key=keys, field=fields)
    def hget(self, key, field):
        self._compare('hget', key, field)

    @rule(key=keys)
    def hgetall(self, key):
        self._compare('hgetall', key)

    @rule(key=keys, field=fields, increment=st.integers())
    def hincrby(self, key, field, increment):
        self._compare('hincrby', key, field, increment)

    # TODO: add a test for hincrbyfloat. See incrbyfloat for why this is
    # problematic

    @rule(key=keys)
    def hkeys(self, key):
        self._compare('hkeys', key)

    @rule(key=keys)
    def hlen(self, key):
        self._compare('hlen', key)

    @rule(key=keys, field=st.lists(fields))
    def hmget(self, key, field):
        self._compare('hmget', key, *field)

    @rule(key=keys, items=st.dictionaries(fields, values))
    def hmset(self, key, items):
        self._compare('hmset', key, items)

    @rule(key=keys, field=fields, value=values)
    def hset(self, key, field, value):
        self._compare('hset', key, field, value)

    @rule(key=keys, field=fields, value=values)
    def hsetnx(self, key, field, value):
        self._compare('hsetnx', key, field, value)

    @rule(key=keys, field=fields)
    def hstrlen(self, key, field):
        self._compare('hstrlen', key, field)

    @rule(key=keys)
    def hvals(self, key):
        self._compare('hvals', key)


TestHash = HashMachine.TestCase


class ListMachine(BaseMachine):
    keys = BaseMachine.keys
    values = BaseMachine.values

    # TODO: blocking commands

    @rule(key=keys, index=counts)
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

    @rule(key=keys, start=counts, stop=counts)
    def lrange(self, key, start, stop):
        self._compare('lrange', key, start, stop)

    @rule(key=keys, count=counts, value=values)
    def lrem(self, key, count, value):
        self._compare('lrem', key, count, value)

    @rule(key=keys, index=counts, value=values)
    def lset(self, key, index, value):
        self._compare('lset', key, index, value)

    @rule(key=keys, start=counts, stop=counts)
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


TestList = ListMachine.TestCase


class SetMachine(BaseMachine):
    keys = BaseMachine.keys
    fields = BaseMachine.fields

    @rule(key=keys, members=st.lists(fields))
    def sadd(self, key, members):
        self._compare('sadd', key, *members)

    @rule(key=keys)
    def scard(self, key):
        self._compare('scard', key)

    @rule(key=st.lists(keys), op=st.sampled_from(['sdiff', 'sinter', 'sunion']))
    def setop(self, key, op):
        self._compare(op, *key, normalize=sort_list)

    @rule(dst=keys, key=st.lists(keys),
          op=st.sampled_from(['sdiffstore', 'sinterstore', 'sunionstore']))
    def setopstore(self, dst, key, op):
        self._compare(op, dst, *key)

    @rule(key=keys, member=fields)
    def sismember(self, key, member):
        self._compare('sismember', key, member)

    @rule(key=keys)
    def smembers(self, key):
        self._compare('smembers', key, normalize=sort_list)

    @rule(src=keys, dst=keys, member=fields)
    def smove(self, src, dst, member):
        self._compare('smove', src, dst, member)

    @rule(key=keys, member=st.lists(fields))
    def srem(self, key, member):
        self._compare('srem', key, *member)


TestSet = SetMachine.TestCase


class ZSetMachine(BaseMachine):
    keys = BaseMachine.keys
    fields = BaseMachine.fields
    scores = hypothesis.stateful.Bundle('scores')

    score_tests = scores | st.builds(lambda x: b'(' + repr(x).encode(), scores)
    string_tests = (
        st.sampled_from([b'+', b'-'])
        | st.builds(operator.add, st.sampled_from([b'(', b'[']), fields))

    @rule(target=scores, value=st.floats(width=32))
    def make_score(self, value):
        return value

    @rule(key=keys, items=st.lists(st.tuples(scores, fields)))
    def zadd(self, key, items):
        # TODO: test xx, nx, ch, incr
        # TODO: support redis-py 3
        flat_items = itertools.chain(*items)
        self._compare('zadd', key, *flat_items)

    @rule(key=keys)
    def zcard(self, key):
        self._compare('zcard', key)

    @rule(key=keys, min=score_tests, max=score_tests)
    def zcount(self, key, min, max):
        self._compare('zcount', key, min, max)

    @rule(key=keys, increment=scores, member=fields)
    def zincrby(self, key, increment, member):
        self._compare('zincrby', key, member, increment)

    @rule(key=keys, start=counts, stop=counts, withscores=st.booleans(), reverse=st.booleans())
    def zrange(self, key, start, stop, withscores, reverse):
        extra = ['withscores'] if withscores else []
        cmd = 'zrevrange' if reverse else 'zrange'
        self._compare(cmd, key, start, stop, *extra)

    @rule(key=keys, min=score_tests, max=score_tests, withscores=st.booleans(),
          limit=st.none() | st.tuples(counts, counts), reverse=st.booleans())
    def zrangebyscore(self, key, min, max, limit, withscores, reverse):
        extra = ['limit', limit[0], limit[1]] if limit else []
        if withscores:
            extra.append('withscores')
        cmd = 'zrevrangebyscore' if reverse else 'zrangebyscore'
        self._compare(cmd, key, min, max, *extra)

    @rule(key=keys, member=fields, reverse=st.booleans())
    def zrank(self, key, member, reverse):
        cmd = 'zrevrank' if reverse else 'zrank'
        self._compare(cmd, key, member)

    @rule(key=keys, member=st.lists(fields))
    def zrem(self, key, member):
        self._compare('zrem', key, *member)

    @rule(key=keys, start=counts, stop=counts)
    def zrembyrank(self, key, start, stop):
        self._compare('zremrangebyrank', key, start, stop)

    @rule(key=keys, member=fields)
    def zscore(self, key, member):
        self._compare('zscore', key, member)

    # TODO: zscan, zunionstore, zinterstore, probably more


TestZSet = ZSetMachine.TestCase


class ZSetNoScoresMachine(BaseMachine):
    keys = BaseMachine.keys
    fields = BaseMachine.fields

    string_tests = (
        st.sampled_from([b'+', b'-'])
        | st.builds(operator.add, st.sampled_from([b'(', b'[']), fields))

    @rule(key=keys, items=st.lists(fields))
    def zadd_zero_score(self, key, items):
        # TODO: test xx, nx, ch, incr
        # TODO: support redis-py 3
        flat_items = itertools.chain(*[(0, item) for item in items])
        self._compare('zadd', key, *flat_items)

    @rule(key=keys, min=string_tests, max=string_tests)
    def zlexcount(self, key, min, max):
        self._compare('zlexcount', key, min, max)

    @rule(key=keys, min=string_tests, max=string_tests,
          limit=st.none() | st.tuples(counts, counts),
          reverse=st.booleans())
    def zrangebylex(self, key, min, max, limit, reverse):
        cmd = 'zrevrangebylex' if reverse else 'zrangebylex'
        if limit is None:
            self._compare(cmd, key, min, max)
        else:
            start, count = limit
            self._compare(cmd, key, min, max, 'limit', start, count)


TestZSetNoScores = ZSetNoScoresMachine.TestCase


class TransactionMachine(StringMachine):
    keys = BaseMachine.keys

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


TestTransaction = TransactionMachine.TestCase


class ServerMachine(StringMachine):
    # TODO: real redis raises an error if there is a save already in progress.
    # Find a better way to test this.
    # @rule()
    # def bgsave(self):
    #     self._compare('bgsave')

    @rule(asynchronous=st.booleans())
    def flushdb(self, asynchronous):
        extra = ['async'] if asynchronous else []
        self._compare('flushdb', *extra)

    @rule(asynchronous=st.booleans())
    def flushall(self, asynchronous):
        extra = ['async'] if asynchronous else []
        self._compare('flushall', *extra)

    # TODO: result is non-deterministic
    # @rule()
    # def lastsave(self):
    #     self._compare('lastsave')

    @rule()
    def save(self):
        self._compare('save')


TestServer = ServerMachine.TestCase


class JointMachine(TransactionMachine, ServerMachine, ConnectionMachine,
                   StringMachine, HashMachine, ListMachine,
                   SetMachine, ZSetMachine):
    # TODO: rule inheritance isn't working!

    # redis-py splits the command on spaces, and hangs if that ends up
    # being an empty list
    @rule(command=st.text().filter(lambda x: bool(x.split())),
          args=st.lists(st.binary() | st.text()))
    def bad_command(self, command, args):
        self._compare(command, *args)

    # TODO: introduce rule for SORT. It'll need a rather complex
    # strategy to cover all the cases.


TestJoint = JointMachine.TestCase
