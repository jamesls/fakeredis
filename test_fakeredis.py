#!/usr/bin/env python

import unittest2 as unittest
import inspect
from functools import wraps

from nose.plugins.skip import SkipTest
import redis
import redis.client

import fakeredis


def redis_must_be_running(cls):
    # This can probably be improved.  This will determines
    # at import time if the tests should be run, but we probably
    # want it to be when the tests are actually run.
    try:
        r = redis.Redis('localhost', port=6379)
        r.ping()
    except redis.ConnectionError:
        redis_running = False
    else:
        redis_running = True
    if not redis_running:
        for name, attr in inspect.getmembers(cls):
            if name.startswith('test_'):
                @wraps(attr)
                def skip_test(*args, **kwargs):
                    raise SkipTest("Redis is not running.")
                setattr(cls, name, skip_test)
        cls.setUp = lambda x: None
        cls.tearDown = lambda x: None
    return cls


class TestFakeRedis(unittest.TestCase):
    def setUp(self):
        self.redis = self.create_redis()

    def tearDown(self):
        self.redis.flushall()

    def create_redis(self, db=0):
        return fakeredis.FakeRedis(db=db)

    def test_flushdb(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.keys(), ['foo'])
        self.assertEqual(self.redis.flushdb(), True)
        self.assertEqual(self.redis.keys(), [])

    def test_set_then_get(self):
        self.assertEqual(self.redis.set('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'), 'bar')

    def test_get_does_not_exist(self):
        self.assertEqual(self.redis.get('foo'), None)

    def test_getbit(self):
        self.redis.setbit('foo', 3, 1)
        self.assertEqual(self.redis.getbit('foo', 0), 0)
        self.assertEqual(self.redis.getbit('foo', 1), 0)
        self.assertEqual(self.redis.getbit('foo', 2), 0)
        self.assertEqual(self.redis.getbit('foo', 3), 1)
        self.assertEqual(self.redis.getbit('foo', 4), 0)
        self.assertEqual(self.redis.getbit('foo', 100), 0)

    def test_multiple_bits_set(self):
        self.redis.setbit('foo', 1, 1)
        self.redis.setbit('foo', 3, 1)
        self.redis.setbit('foo', 5, 1)

        self.assertEqual(self.redis.getbit('foo', 0), 0)
        self.assertEqual(self.redis.getbit('foo', 1), 1)
        self.assertEqual(self.redis.getbit('foo', 2), 0)
        self.assertEqual(self.redis.getbit('foo', 3), 1)
        self.assertEqual(self.redis.getbit('foo', 4), 0)
        self.assertEqual(self.redis.getbit('foo', 5), 1)
        self.assertEqual(self.redis.getbit('foo', 6), 0)

    def test_unset_bits(self):
        self.redis.setbit('foo', 1, 1)
        self.redis.setbit('foo', 2, 0)
        self.redis.setbit('foo', 3, 1)
        self.assertEqual(self.redis.getbit('foo', 1), 1)
        self.redis.setbit('foo', 1, 0)
        self.assertEqual(self.redis.getbit('foo', 1), 0)
        self.redis.setbit('foo', 3, 0)
        self.assertEqual(self.redis.getbit('foo', 3), 0)

    def test_setbits_and_getkeys(self):
        # The bit operations and the get commands
        # should play nicely with each other.
        self.redis.setbit('foo', 1, 1)
        self.assertEqual(self.redis.get('foo'), '@')
        self.redis.setbit('foo', 2, 1)
        self.assertEqual(self.redis.get('foo'), '`')
        self.redis.setbit('foo', 3, 1)
        self.assertEqual(self.redis.get('foo'), 'p')
        self.redis.setbit('foo', 9, 1)
        self.assertEqual(self.redis.get('foo'), 'p@')
        self.redis.setbit('foo', 54, 1)
        self.assertEqual(self.redis.get('foo'), 'p@\x00\x00\x00\x00\x02')

    def test_getset_not_exist(self):
        val = self.redis.getset('foo', 'bar')
        self.assertEqual(val, None)
        self.assertEqual(self.redis.get('foo'), 'bar')

    def test_getset_exists(self):
        self.redis.set('foo', 'bar')
        val = self.redis.getset('foo', 'baz')
        self.assertEqual(val, 'bar')

    def test_setitem_getitem(self):
        self.assertEqual(self.redis.keys(), [])
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis['foo'], 'bar')

    def test_strlen(self):
        self.redis['foo'] = 'bar'

        self.assertEqual(self.redis.strlen('foo'), 3)
        self.assertEqual(self.redis.strlen('noexists'), 0)

    def test_substr(self):
        self.redis['foo'] = 'one_two_three'
        self.assertEqual(self.redis.substr('foo', 0), 'one_two_three')
        self.assertEqual(self.redis.substr('foo', 0, 2), 'one')
        self.assertEqual(self.redis.substr('foo', 4, 6), 'two')
        self.assertEqual(self.redis.substr('foo', -5), 'three')

    def test_substr_noexist_key(self):
        self.assertEqual(self.redis.substr('foo', 0), '')
        self.assertEqual(self.redis.substr('foo', 10), '')
        self.assertEqual(self.redis.substr('foo', -5, -1), '')

    def test_append(self):
        self.assertTrue(self.redis.set('foo', 'bar'))
        self.assertEqual(self.redis.append('foo', 'baz'), 6)
        self.assertEqual(self.redis.get('foo'), 'barbaz')

    def test_incr_with_no_preexisting_key(self):
        self.assertEqual(self.redis.incr('foo'), 1)
        self.assertEqual(self.redis.incr('bar', 2), 2)

    def test_incr_preexisting_key(self):
        self.redis.set('foo', 15)
        self.assertEqual(self.redis.incr('foo', 5), 20)
        self.assertEqual(self.redis.get('foo'), '20')

    def test_incr_bad_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.incr('foo', 15)

    def test_decr(self):
        self.redis.set('foo', 10)
        self.assertEqual(self.redis.decr('foo'), 9)
        self.assertEqual(self.redis.get('foo'), '9')

    def test_decr_newkey(self):
        self.redis.decr('foo')
        self.assertEqual(self.redis.get('foo'), '-1')

    def test_decr_badtype(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.decr('foo', 15)

    def test_exists(self):
        self.assertFalse('foo' in self.redis)
        self.redis.set('foo', 'bar')
        self.assertTrue('foo' in self.redis)

    def test_contains(self):
        self.assertFalse(self.redis.exists('foo'))
        self.redis.set('foo', 'bar')
        self.assertTrue(self.redis.exists('foo'))

    def test_rename(self):
        self.redis.set('foo', 'unique value')
        self.assertTrue(self.redis.rename('foo', 'bar'))
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.get('bar'), 'unique value')

    def test_rename_nonexistent_key(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.rename('foo', 'bar')

    def test_renamenx_doesnt_exist(self):
        self.redis.set('foo', 'unique value')
        self.assertTrue(self.redis.renamenx('foo', 'bar'))
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.get('bar'), 'unique value')

    def test_rename_does_exist(self):
        self.redis.set('foo', 'unique value')
        self.redis.set('bar', 'unique value2')
        self.assertFalse(self.redis.renamenx('foo', 'bar'))
        self.assertEqual(self.redis.get('foo'), 'unique value')
        self.assertEqual(self.redis.get('bar'), 'unique value2')

    def test_mget(self):
        self.redis.set('foo', 'one')
        self.redis.set('bar', 'two')
        self.assertEqual(self.redis.mget(['foo', 'bar']), ['one', 'two'])
        self.assertEqual(self.redis.mget(['foo', 'bar', 'baz']),
                         ['one', 'two', None])
        self.assertEqual(self.redis.mget('foo', 'bar'), ['one', 'two'])
        self.assertEqual(self.redis.mget('foo', 'bar', None),
                         ['one', 'two', None])

    def test_mset(self):
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mget('foo', 'bar'), ['one', 'two'])

    def test_msetnx(self):
        self.assertEqual(self.redis.msetnx({'foo': 'one', 'bar': 'two'}),
                         True)
        self.assertEqual(self.redis.msetnx({'bar': 'two', 'baz': 'three'}),
                         False)
        self.assertEqual(self.redis.mget('foo', 'bar', 'baz'),
                         ['one', 'two', None])

    def test_setnx(self):
        self.assertEqual(self.redis.setnx('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'),  'bar')
        self.assertEqual(self.redis.setnx('foo', 'baz'), False)
        self.assertEqual(self.redis.get('foo'),  'bar')

    def test_delete(self):
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis.delete('foo'), True)
        self.assertEqual(self.redis.get('foo'), None)

    def test_delete_multiple(self):
        self.redis['one'] = 'one'
        self.redis['two'] = 'two'
        self.redis['three'] = 'three'
        self.assertEqual(self.redis.delete('one', 'two'), True)
        self.assertEqual(self.redis.get('one'), None)
        self.assertEqual(self.redis.get('two'), None)
        self.assertEqual(self.redis.get('three'), 'three')
        self.assertEqual(self.redis.delete('one', 'two'), False)
        # If any keys are deleted, True is returned.
        self.assertEqual(self.redis.delete('two', 'three'), True)
        self.assertEqual(self.redis.get('three'), None)

    def test_delete_nonexistent_key(self):
        self.assertEqual(self.redis.delete('foo'), False)

    ## Tests for the list type.

    def test_lpush_then_lrange_all(self):
        self.assertEqual(self.redis.lpush('foo', 'bar'), 1)
        self.assertEqual(self.redis.lpush('foo', 'baz'), 2)
        self.assertEqual(self.redis.lrange('foo', 0, -1), ['baz', 'bar'])

    def test_lpush_then_lrange_portion(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'three')
        self.redis.lpush('foo', 'four')
        self.assertEqual(self.redis.lrange('foo', 0, 2),
                         ['four', 'three', 'two'])
        self.assertEqual(self.redis.lrange('foo', 0, 3),
                         ['four', 'three', 'two', 'one'])

    def test_lpush_key_does_not_exist(self):
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_llen(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'three')
        self.assertEqual(self.redis.llen('foo'), 3)

    def test_llen_no_exist(self):
        self.assertEqual(self.redis.llen('foo'), 0)

    def test_lrem_postitive_count(self):
        self.redis.lpush('foo', 'same')
        self.redis.lpush('foo', 'same')
        self.redis.lpush('foo', 'different')
        self.redis.lrem('foo', 'same', 2)
        self.assertEqual(self.redis.lrange('foo', 0, -1), ['different'])

    def test_lrem_negative_count(self):
        self.redis.lpush('foo', 'removeme')
        self.redis.lpush('foo', 'three')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'removeme')
        self.redis.lrem('foo', 'removeme', -1)
        # Should remove it from the end of the list,
        # leaving the 'removeme' from the front of the list alone.
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         ['removeme', 'one', 'two', 'three'])

    def test_lrem_zero_count(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 'one', 0)
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_lrem_default_value(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_lrem_does_not_exist(self):
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 'one')
        # These should be noops.
        self.redis.lrem('foo', 'one', -2)
        self.redis.lrem('foo', 'one', 2)

    def test_lrem_return_value(self):
        self.redis.lpush('foo', 'one')
        count = self.redis.lrem('foo', 'one')
        self.assertEqual(count, 1)
        self.assertEqual(self.redis.lrem('foo', 'one'), 0)

    def test_rpush(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         ['one', 'two', 'three'])

    def test_lpop(self):
        self.assertEqual(self.redis.rpush('foo', 'one'), 1)
        self.assertEqual(self.redis.rpush('foo', 'two'), 2)
        self.assertEqual(self.redis.rpush('foo', 'three'), 3)
        self.assertEqual(self.redis.lpop('foo'), 'one')
        self.assertEqual(self.redis.lpop('foo'), 'two')
        self.assertEqual(self.redis.lpop('foo'), 'three')

    def test_lpop_empty_list(self):
        self.redis.rpush('foo', 'one')
        self.redis.lpop('foo')
        self.assertEqual(self.redis.lpop('foo'), None)
        # Verify what happens if we try to pop from a key
        # we've never seen before.
        self.assertEqual(self.redis.lpop('noexists'), None)

    def test_lset(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.redis.lset('foo', 0, 'four')
        self.redis.lset('foo', -2, 'five')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         ['four', 'five', 'three'])

    def test_lset_index_out_of_range(self):
        self.redis.rpush('foo', 'one')
        with self.assertRaises(redis.ResponseError):
            self.redis.lset('foo', 3, 'three')

    def test_rpushx(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpushx('foo', 'two')
        self.redis.rpushx('bar', 'three')
        self.assertEqual(self.redis.lrange('foo', 0, -1), ['one', 'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_lindex(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.lindex('foo', 0), 'one')
        self.assertEqual(self.redis.lindex('foo', 4), None)
        self.assertEqual(self.redis.lindex('bar', 4), None)

    def test_lpushx(self):
        self.redis.lpush('foo', 'two')
        self.redis.lpushx('foo', 'one')
        self.redis.lpushx('bar', 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), ['one', 'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_rpop(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.rpop('foo'), 'two')
        self.assertEqual(self.redis.rpop('foo'), 'one')
        self.assertEqual(self.redis.rpop('foo'), None)

    def test_linsert(self):
        self.redis.rpush('foo', 'hello')
        self.redis.rpush('foo', 'world')
        self.redis.linsert('foo', 'before', 'world', 'there')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         ['hello', 'there', 'world'])

    def test_rpoplpush(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('bar', 'one')

        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), 'two')
        self.assertEqual(self.redis.lrange('foo', 0, -1), ['one'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), ['two', 'one'])

    def test_blpop_single_list(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.assertEqual(self.redis.blpop(['foo'], timeout=1),
                         ('foo', 'one'))

    def test_blpop_test_multiple_lists(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        # bar has nothing, so the returned value should come
        # from foo.
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         ('foo', 'one'))
        self.redis.rpush('bar', 'three')
        # bar now has something, so the returned value should come
        # from bar.
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         ('bar', 'three'))
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         ('foo', 'two'))

    def test_blpop_allow_single_key(self):
        # blpop converts single key arguments to a one element list.
        self.redis.rpush('foo', 'one')
        self.assertEqual(self.redis.blpop('foo', timeout=1), ('foo', 'one'))

    def test_brpop_test_multiple_lists(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpop(['bar', 'foo'], timeout=1),
                         ('foo', 'two'))

    def test_brpop_single_key(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpop('foo', timeout=1),
                         ('foo', 'two'))

    def test_brpoplpush_multi_keys(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpoplpush('foo', 'bar', timeout=1),
                         'two')
        self.assertEqual(self.redis.lrange('bar', 0, -1), ['two'])

    ## Tests for the hash type.

    def test_hset_then_hget(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        self.assertEqual(self.redis.hget('foo', 'key'), 'value')

    def test_hgetall(self):
        self.assertEqual(self.redis.hset('foo', 'k1', 'v1'), 1)
        self.assertEqual(self.redis.hset('foo', 'k2', 'v2'), 1)
        self.assertEqual(self.redis.hset('foo', 'k3', 'v3'), 1)
        self.assertEqual(self.redis.hgetall('foo'), {'k1': 'v1', 'k2': 'v2',
                                                     'k3': 'v3'})

    def test_hgetall_empty_key(self):
        self.assertEqual(self.redis.hgetall('foo'), {})

    def test_hexists(self):
        self.redis.hset('foo', 'bar', 'v1')
        self.assertEqual(self.redis.hexists('foo', 'bar'), 1)
        self.assertEqual(self.redis.hexists('foo', 'baz'), 0)
        self.assertEqual(self.redis.hexists('bar', 'bar'), 0)

    def test_hkeys(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(set(self.redis.hkeys('foo')), set(['k1', 'k2']))
        self.assertEqual(set(self.redis.hkeys('bar')), set([]))

    def test_hlen(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(self.redis.hlen('foo'), 2)

    def test_hvals(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(set(self.redis.hvals('foo')), set(['v1', 'v2']))
        self.assertEqual(set(self.redis.hvals('bar')), set([]))

    def test_hmget(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.redis.hset('foo', 'k3', 'v3')
        # Normal case.
        self.assertEqual(self.redis.hmget('foo', ['k1', 'k3']), ['v1', 'v3'])
        # Key does not exist.
        self.assertEqual(self.redis.hmget('bar', ['k1', 'k3']), [None, None])
        # Some keys in the hash do not exist.
        self.assertEqual(self.redis.hmget('foo', ['k1', 'k500']),
                         ['v1', None])

    def test_hdel(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.assertEqual(self.redis.hget('foo', 'k1'), 'v1')
        self.assertEqual(self.redis.hdel('foo', 'k1'), True)
        self.assertEqual(self.redis.hget('foo', 'k1'), None)
        self.assertEqual(self.redis.hdel('foo', 'k1'), False)

    def test_hincrby(self):
        self.redis.hset('foo', 'counter', 0)
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 1)
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 2)
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 3)

    def test_hincrby_with_no_starting_value(self):
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 1)
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 2)
        self.assertEqual(self.redis.hincrby('foo', 'counter'), 3)

    def test_hincrby_with_range_param(self):
        self.assertEqual(self.redis.hincrby('foo', 'counter', 2), 2)
        self.assertEqual(self.redis.hincrby('foo', 'counter', 2), 4)
        self.assertEqual(self.redis.hincrby('foo', 'counter', 2), 6)

    def test_hsetnx(self):
        self.assertEqual(self.redis.hsetnx('foo', 'newkey', 'v1'), True)
        self.assertEqual(self.redis.hsetnx('foo', 'newkey', 'v1'), False)
        self.assertEqual(self.redis.hget('foo', 'newkey'), 'v1')

    def test_hmsetset_empty_raises_error(self):
        with self.assertRaises(redis.DataError):
            self.redis.hmset('foo', {})

    def test_hmsetset(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.assertEqual(self.redis.hmset('foo', {'k2': 'v2', 'k3': 'v3'}),
                         True)

    def test_sadd(self):
        self.assertEqual(self.redis.sadd('foo', 'member1'), True)
        self.assertEqual(self.redis.sadd('foo', 'member1'), False)
        self.assertEqual(self.redis.smembers('foo'), set(['member1']))

    def test_scard(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.scard('foo'), 2)

    def test_sdiff(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sdiff('foo', 'bar'), set(['member1']))

    def test_sdiff_one_key(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.sdiff('foo'), set(['member1', 'member2']))

    def test_sdiff_empty(self):
        self.assertEqual(self.redis.sdiff('foo'), set())

    def test_sdiffstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sdiffstore('baz', 'foo', 'bar'), 1)

    def test_sinter(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sinter('foo', 'bar'), set(['member2']))
        self.assertEqual(self.redis.sinter('foo'), set(['member1', 'member2']))

    def test_sinterstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sinterstore('baz', 'foo', 'bar'), 1)

    def test_sismember(self):
        self.assertEqual(self.redis.sismember('foo', 'member1'), False)
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.sismember('foo', 'member1'), True)

    def test_smove(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), True)
        self.assertEqual(self.redis.smembers('bar'), set(['member1']))

    def test_smove_non_existent_key(self):
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), False)

    def test_spop(self):
        # This is tricky because it pops a random element.
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.spop('foo'), 'member1')
        self.assertEqual(self.redis.spop('foo'), None)

    def test_srandmember(self):
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.srandmember('foo'), 'member1')
        # Shouldn't be removed from the set.
        self.assertEqual(self.redis.srandmember('foo'), 'member1')

    def test_srem(self):
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.smembers('foo'), set(['member1']))
        self.assertEqual(self.redis.srem('foo', 'member1'), True)
        self.assertEqual(self.redis.smembers('foo'), set([]))
        self.assertEqual(self.redis.srem('foo', 'member1'), False)

    def test_sunion(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunion('foo', 'bar'),
                         set(['member1', 'member2', 'member3']))

    def test_sunionstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunionstore('baz', 'foo', 'bar'), 3)
        self.assertEqual(self.redis.smembers('baz'),
                         set(['member1', 'member2', 'member3']))

    def test_zadd(self):
        self.redis.zadd('foo', three=3)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', one=1)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         ['one', 'two', 'three'])

    def test_zadd_deprecated(self):
        self.redis.zadd('foo', 'one', 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         ['one'])

    def test_zrange_same_score(self):
        self.redis.zadd('foo', two_a=2)
        self.redis.zadd('foo', two_b=2)
        self.redis.zadd('foo', two_c=2)
        self.redis.zadd('foo', two_d=2)
        self.redis.zadd('foo', two_e=2)
        self.assertEqual(self.redis.zrange('foo', 2, 3),
                         ['two_c', 'two_d'])

    def test_zcard(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.assertEqual(self.redis.zcard('foo'), 2)

    def test_zcard_non_existent_key(self):
        self.assertEqual(self.redis.zcard('foo'), 0)

    def test_zcount(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', three=2)
        self.redis.zadd('foo', five=5)
        self.assertEqual(self.redis.zcount('foo', 2, 4), 1)
        self.assertEqual(self.redis.zcount('foo', 1, 4), 2)
        self.assertEqual(self.redis.zcount('foo', 0, 5), 3)

    def test_zincrby(self):
        self.redis.zadd('foo', one=1)
        self.assertEqual(self.redis.zincrby('foo', 'one', 10), 11)
        self.assertEqual(self.redis.zrange('foo', 0, -1, withscores=True),
                         [('one', 11)])

    def test_zrange_descending(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, -1, desc=True),
                         ['three', 'two', 'one'])

    def test_zrange_descending_with_scores(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, -1, desc=True,
                                           withscores=True),
                         [('three', 3), ('two', 2), ('one', 1)])

    def test_zrange_with_positive_indices(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, 1), ['one', 'two'])

    def test_zrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrank('foo', 'one'), 0)
        self.assertEqual(self.redis.zrank('foo', 'two'), 1)
        self.assertEqual(self.redis.zrank('foo', 'three'), 2)

    def test_zrank_non_existent_member(self):
        self.assertEqual(self.redis.zrank('foo', 'one'), None)

    def test_zrem(self):
        self.redis.zadd('foo', one=1)
        self.assertTrue(self.redis.zrem('foo', 'one'))
        self.assertEqual(self.redis.zrange('foo', 0, -1), [])

    def test_zrem_non_existent_member(self):
        self.assertFalse(self.redis.zrem('foo', 'one'))

    def test_zscore(self):
        self.redis.zadd('foo', one=54)
        self.assertEqual(self.redis.zscore('foo', 'one'), 54)

    def test_zscore_non_existent_member(self):
        self.assertIsNone(self.redis.zscore('foo', 'one'))

    def test_zrevrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrank('foo', 'one'), 2)
        self.assertEqual(self.redis.zrevrank('foo', 'two'), 1)
        self.assertEqual(self.redis.zrevrank('foo', 'three'), 0)

    def test_zrevrank_non_existent_member(self):
        self.assertEqual(self.redis.zrevrank('foo', 'one'), None)

    def test_zrevrange(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrange('foo', 0, 1), ['three', 'two'])
        self.assertEqual(self.redis.zrevrange('foo', 0, -1),
                         ['three', 'two', 'one'])

    def test_zrangebyscore(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', two_a_also=2)
        self.redis.zadd('foo', two_b_also=2)
        self.redis.zadd('foo', four=4)
        self.assertEqual(self.redis.zrangebyscore('foo', 1, 3),
                         ['two', 'two_a_also', 'two_b_also'])
        self.assertEqual(self.redis.zrangebyscore('foo', 2, 3),
                         ['two', 'two_a_also', 'two_b_also'])
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4),
                         ['zero', 'two', 'two_a_also', 'two_b_also', 'four'])

    def test_zrangebyscore_slice(self):
        self.redis.zadd('foo', two_a=2)
        self.redis.zadd('foo', two_b=2)
        self.redis.zadd('foo', two_c=2)
        self.redis.zadd('foo', two_d=2)
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4, 0, 2),
                         ['two_a', 'two_b'])
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4, 1, 3),
                         ['two_b', 'two_c', 'two_d'])

    def test_zrangebyscore_withscores(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrangebyscore('foo', 1, 3, 0, 2, True),
                         [('one', 1), ('two', 2)])

    def test_zrevrangebyscore(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1),
                         ['three', 'two', 'one'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 2),
                         ['three', 'two'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1, 0, 1),
                         ['three'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1, 1, 2),
                         ['two', 'one'])

    def test_zremrangebyrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zremrangebyrank('foo', 0, 1), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), ['three'])

    def test_zremrangebyrank_negative_indices(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zremrangebyrank('foo', -2, -1), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), ['one'])

    def test_zremrangebyrank_out_of_bounds(self):
        self.redis.zadd('foo', one=1)
        self.assertEqual(self.redis.zremrangebyrank('foo', 1, 3), 0)

    def test_zremrangebyscore(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', four=4)
        # Outside of range.
        self.assertEqual(self.redis.zremrangebyscore('foo', 5, 10), 0)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         ['zero', 'two', 'four'])
        # Middle of range.
        self.assertEqual(self.redis.zremrangebyscore('foo', 1, 3), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), ['zero', 'four'])
        self.assertEqual(self.redis.zremrangebyscore('foo', 1, 3), 0)
        # Entire range.
        self.assertEqual(self.redis.zremrangebyscore('foo', 0, 4), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [])

    def test_zremrangebyscore_badkey(self):
        self.assertEqual(self.redis.zremrangebyscore('foo', 0, 2), 0)

    def test_zunionstore_sum(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 2), ('three', 3), ('two', 4)])

    def test_zunionstore_max(self):
        self.redis.zadd('foo', one=0)
        self.redis.zadd('foo', two=0)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 1), ('two', 2), ('three', 3)])

    def test_zunionstore_min(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=0)
        self.redis.zadd('bar', two=0)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='MIN')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 0), ('two', 0), ('three', 3)])

    def test_zunionstore_weights(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', four=4)
        self.redis.zunionstore('baz', {'foo': 1, 'bar': 2}, aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 3), ('two', 6), ('four', 8)])

    def test_zunionstore_badkey(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 1), ('two', 2)])
        self.redis.zunionstore('baz', {'foo': 1, 'bar': 2}, aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 1), ('two', 2)])

    def test_zinterstore(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zinterstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 2), ('two', 4)])

    def test_zinterstore_max(self):
        self.redis.zadd('foo', one=0)
        self.redis.zadd('foo', two=0)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zinterstore('baz', ['foo', 'bar'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 1), ('two', 2)])

    def test_zinterstore_onekey(self):
        self.redis.zadd('foo', one=1)
        self.redis.zinterstore('baz', ['foo'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [('one', 1)])

    def test_zinterstore_nokey(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.zinterstore('baz', [], aggregate='MAX')

    def test_zunionstore_nokey(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.zunionstore('baz', [], aggregate='MAX')

    def test_multidb(self):
        r1 = self.create_redis(db=0)
        r2 = self.create_redis(db=1)

        r1['r1'] = 'r1'
        r2['r2'] = 'r2'

        self.assertTrue('r2' not in r1)
        self.assertTrue('r1' not in r2)

        self.assertEqual(r1['r1'], 'r1')
        self.assertEqual(r2['r2'], 'r2')

        r1.flushall()

        self.assertTrue('r1' not in r1)
        self.assertTrue('r2' not in r2)

    def test_basic_sort(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo'), ['1', '2', '3'])

    def test_empty_sort(self):
        self.assertEqual(self.redis.sort('foo'), [])

    def test_sort_range_offset_range(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo', start=0, num=2), ['1', '2'])

    def test_sort_range_offset_norange(self):
        with self.assertRaises(redis.RedisError):
            self.redis.sort('foo', start=1)

    def test_sort_range_with_large_range(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')
        # num=20 even though len(foo) is 4.
        self.assertEqual(self.redis.sort('foo', start=1, num=20),
                         ['2', '3', '4'])

    def test_sort_descending(self):
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '3')
        self.assertEqual(self.redis.sort('foo', desc=True), ['3', '2', '1'])

    def test_sort_alpha(self):
        self.redis.rpush('foo', '2a')
        self.redis.rpush('foo', '1b')
        self.redis.rpush('foo', '2b')
        self.redis.rpush('foo', '1a')

        self.assertEqual(self.redis.sort('foo', alpha=True),
                         ['1a', '1b', '2a', '2b'])
        self.assertEqual(self.redis.sort('foo', alpha=False),
                         ['1b', '1a', '2a', '2b'])

    def test_sort_with_store_option(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo', store='bar'), 4)
        self.assertEqual(self.redis.lrange('bar', 0, -1),
                         ['1', '2', '3', '4'])

    def test_sort_with_by_and_get_option(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.redis['weight_1'] = '4'
        self.redis['weight_2'] = '3'
        self.redis['weight_3'] = '2'
        self.redis['weight_4'] = '1'

        self.redis['data_1'] = 'one'
        self.redis['data_2'] = 'two'
        self.redis['data_3'] = 'three'
        self.redis['data_4'] = 'four'

        self.assertEqual(self.redis.sort('foo', by='weight_*', get='data_*'),
                         ['four', 'three', 'two', 'one'])
        self.assertEqual(self.redis.sort('foo', by='weight_*', get='#'),
                         ['4', '3', '2', '1'])
        self.assertEqual(
            self.redis.sort('foo', by='weight_*', get=('data_*', '#')),
            ['four', '4', 'three', '3', 'two', '2', 'one', '1'])
        self.assertEqual(self.redis.sort('foo', by='weight_*', get='data_1'),
                         [None, None, None, None])

    def test_sort_with_hash(self):
        self.redis.rpush('foo', 'middle')
        self.redis.rpush('foo', 'eldest')
        self.redis.rpush('foo', 'youngest')
        self.redis.hset('record_youngest', 'age', 1)
        self.redis.hset('record_youngest', 'name', 'baby')

        self.redis.hset('record_middle', 'age', 10)
        self.redis.hset('record_middle', 'name', 'teen')

        self.redis.hset('record_eldest', 'age', 20)
        self.redis.hset('record_eldest', 'name', 'adult')

        self.assertEqual(self.redis.sort('foo', by='record_*->age'),
                         ['youngest', 'middle', 'eldest'])
        self.assertEqual(
            self.redis.sort('foo', by='record_*->age', get='record_*->name'),
            ['baby', 'teen', 'adult'])

    def test_pipeline(self):
        # The pipeline method returns ann object for
        # issuing multiple commands in a batch.
        p = self.redis.pipeline()
        p.set('foo', 'bar').get('foo')
        p.lpush('baz', 'quux')
        p.lpush('baz', 'quux2').lrange('baz', 0, -1)
        res = p.execute()

        # Check return values returned as list.
        self.assertEqual([True, 'bar', 1, 2, ['quux2', 'quux']], res)

        # Check side effects happened as expected.
        self.assertEqual(['quux2', 'quux'], self.redis.lrange('baz', 0, -1))

    def test_pipeline_non_transational(self):
        # For our simple-minded model I don’t think
        # there is any observable difference.
        p = self.redis.pipeline(transaction=False)
        res = p.set('baz', 'quux').get('baz').execute()

        self.assertEqual([True, 'quux'], res)

    def test_pipeline_raises_when_watched_key_changed(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            p.watch('greet', 'foo', 'quux')
            nextf = p.get('foo') + 'baz'
            # simulate change happening on another thread:
            self.redis.rpush('greet', 'world')
            p.multi() # begin pipelining
            p.set('foo', nextf)

            self.assertRaises(redis.WatchError, p.execute)
        finally:
            p.reset()

    def test_pipeline_succeeds_despite_unwatched_key_changed(self):
        # Same setup as before except for the params to the WATCH command.
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            p.watch('foo') # only watch one of the 2 keys
            nextf = p.get('foo') + 'baz'
            # simulate change happening on another thread:
            self.redis.rpush('greet', 'world')
            p.multi() # begin pipelining
            p.set('foo', nextf)
            p.execute()

            # Check the commands were executed.
            self.assertEqual('barbaz', self.redis.get('foo'))
        finally:
            p.reset()

    def test_pipeline_as_context_manager(self):
        self.redis.set('foo', 'bar')
        with self.redis.pipeline() as p:
            p.watch('foo')
            self.assertTrue(isinstance(p, redis.client.BasePipeline) or p.need_reset)
            p.multi() # begin pipelining
            p.set('foo', 'baz')
            p.execute()

        # Usually you would consider the pipeline to
        # have been destroyed
        # after the with statement, but we need to check
        # it was reset properly:
        self.assertTrue(isinstance(p, redis.client.BasePipeline) or not p.need_reset)

    def test_pipeline_transaction_shortcut(self):
        # This example taken pretty much from the redis-py documetnation.
        self.redis.set('OUR-SEQUENCE-KEY', 13)
        calls = []
        def client_side_incr(pipe):
            calls.append((pipe,))
            current_value = pipe.get('OUR-SEQUENCE-KEY')
            next_value = int(current_value) + 1

            if len(calls) < 3:
                # Simulate a change from another thread.
                self.redis.set('OUR-SEQUENCE-KEY', next_value)

            pipe.multi()
            pipe.set('OUR-SEQUENCE-KEY', next_value)
        res = self.redis.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')

        self.assertEqual([True], res)
        self.assertEqual(16, int(self.redis.get('OUR-SEQUENCE-KEY')))
        self.assertEqual(3, len(calls))


@redis_must_be_running
class TestRealRedis(TestFakeRedis):
    def create_redis(self, db=0):
        return redis.Redis('localhost', port=6379, db=db)


if __name__ == '__main__':
    unittest.main()
