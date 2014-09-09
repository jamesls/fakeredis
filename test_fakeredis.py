#!/usr/bin/env python
from time import sleep, time
from redis.exceptions import ResponseError
import inspect
from functools import wraps
import sys

from nose.plugins.skip import SkipTest
from nose.plugins.attrib import attr
import redis
import redis.client

import fakeredis
from datetime import datetime, timedelta

PY2 = sys.version_info[0] == 2

if not PY2:
    long = int

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest


def redis_must_be_running(cls):
    # This can probably be improved.  This will determines
    # at import time if the tests should be run, but we probably
    # want it to be when the tests are actually run.
    try:
        r = redis.StrictRedis('localhost', port=6379)
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


class TestFakeStrictRedis(unittest.TestCase):
    def setUp(self):
        self.redis = self.create_redis()

    def tearDown(self):
        self.redis.flushall()
        del self.redis

    if sys.version_info >= (3,):
        def assertItemsEqual(self, a, b):
            return self.assertCountEqual(a, b)

    def create_redis(self, db=0):
        return fakeredis.FakeStrictRedis(db=db)

    def test_flushdb(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.keys(), [b'foo'])
        self.assertEqual(self.redis.flushdb(), True)
        self.assertEqual(self.redis.keys(), [])

    def test_set_then_get(self):
        self.assertEqual(self.redis.set('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_get_does_not_exist(self):
        self.assertEqual(self.redis.get('foo'), None)

    def test_get_with_non_str_keys(self):
        self.assertEqual(self.redis.set('2', 'bar'), True)
        self.assertEqual(self.redis.get(2), b'bar')

    def test_set_non_str_keys(self):
        self.assertEqual(self.redis.set(2, 'bar'), True)
        self.assertEqual(self.redis.get(2), b'bar')
        self.assertEqual(self.redis.get('2'), b'bar')

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
        self.assertEqual(self.redis.get('foo'), b'@')
        self.redis.setbit('foo', 2, 1)
        self.assertEqual(self.redis.get('foo'), b'`')
        self.redis.setbit('foo', 3, 1)
        self.assertEqual(self.redis.get('foo'), b'p')
        self.redis.setbit('foo', 9, 1)
        self.assertEqual(self.redis.get('foo'), b'p@')
        self.redis.setbit('foo', 54, 1)
        self.assertEqual(self.redis.get('foo'), b'p@\x00\x00\x00\x00\x02')

    def test_bitcount(self):
        self.redis.delete('foo')
        self.assertEqual(self.redis.bitcount('foo'), 0)
        self.redis.setbit('foo', 1, 1)
        self.assertEqual(self.redis.bitcount('foo'), 1)
        self.redis.setbit('foo', 8, 1)
        self.assertEqual(self.redis.bitcount('foo'), 2)
        self.assertEqual(self.redis.bitcount('foo', 1, 1), 1)
        self.redis.setbit('foo', 57, 1)
        self.assertEqual(self.redis.bitcount('foo'), 3)
        self.redis.set('foo', ' ')
        self.assertEqual(self.redis.bitcount('foo'), 1)

    def test_getset_not_exist(self):
        val = self.redis.getset('foo', 'bar')
        self.assertEqual(val, None)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_getset_exists(self):
        self.redis.set('foo', 'bar')
        val = self.redis.getset('foo', 'baz')
        self.assertEqual(val, b'bar')

    def test_setitem_getitem(self):
        self.assertEqual(self.redis.keys(), [])
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis['foo'], b'bar')

    def test_strlen(self):
        self.redis['foo'] = 'bar'

        self.assertEqual(self.redis.strlen('foo'), 3)
        self.assertEqual(self.redis.strlen('noexists'), 0)

    def test_substr(self):
        self.redis['foo'] = 'one_two_three'
        self.assertEqual(self.redis.substr('foo', 0), b'one_two_three')
        self.assertEqual(self.redis.substr('foo', 0, 2), b'one')
        self.assertEqual(self.redis.substr('foo', 4, 6), b'two')
        self.assertEqual(self.redis.substr('foo', -5), b'three')

    def test_substr_noexist_key(self):
        self.assertEqual(self.redis.substr('foo', 0), b'')
        self.assertEqual(self.redis.substr('foo', 10), b'')
        self.assertEqual(self.redis.substr('foo', -5, -1), b'')

    def test_append(self):
        self.assertTrue(self.redis.set('foo', 'bar'))
        self.assertEqual(self.redis.append('foo', 'baz'), 6)
        self.assertEqual(self.redis.get('foo'), b'barbaz')

    def test_incr_with_no_preexisting_key(self):
        self.assertEqual(self.redis.incr('foo'), 1)
        self.assertEqual(self.redis.incr('bar', 2), 2)

    def test_incr_preexisting_key(self):
        self.redis.set('foo', 15)
        self.assertEqual(self.redis.incr('foo', 5), 20)
        self.assertEqual(self.redis.get('foo'), b'20')

    def test_incr_bad_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.incr('foo', 15)

    def test_decr(self):
        self.redis.set('foo', 10)
        self.assertEqual(self.redis.decr('foo'), 9)
        self.assertEqual(self.redis.get('foo'), b'9')

    def test_decr_newkey(self):
        self.redis.decr('foo')
        self.assertEqual(self.redis.get('foo'), b'-1')

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
        self.assertEqual(self.redis.get('bar'), b'unique value')

    def test_rename_nonexistent_key(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.rename('foo', 'bar')

    def test_renamenx_doesnt_exist(self):
        self.redis.set('foo', 'unique value')
        self.assertTrue(self.redis.renamenx('foo', 'bar'))
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.get('bar'), b'unique value')

    def test_rename_does_exist(self):
        self.redis.set('foo', 'unique value')
        self.redis.set('bar', 'unique value2')
        self.assertFalse(self.redis.renamenx('foo', 'bar'))
        self.assertEqual(self.redis.get('foo'), b'unique value')
        self.assertEqual(self.redis.get('bar'), b'unique value2')

    def test_mget(self):
        self.redis.set('foo', 'one')
        self.redis.set('bar', 'two')
        self.assertEqual(self.redis.mget(['foo', 'bar']), [b'one', b'two'])
        self.assertEqual(self.redis.mget(['foo', 'bar', 'baz']),
                         [b'one', b'two', None])
        self.assertEqual(self.redis.mget('foo', 'bar'), [b'one', b'two'])
        self.assertEqual(self.redis.mget('foo', 'bar', None),
                         [b'one', b'two', None])

    def test_mset(self):
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mget('foo', 'bar'), [b'one', b'two'])

    def test_msetnx(self):
        self.assertEqual(self.redis.msetnx({'foo': 'one', 'bar': 'two'}),
                         True)
        self.assertEqual(self.redis.msetnx({'bar': 'two', 'baz': 'three'}),
                         False)
        self.assertEqual(self.redis.mget('foo', 'bar', 'baz'),
                         [b'one', b'two', None])

    def test_setex(self):
        self.assertEqual(self.redis.setex('foo', 100, 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_setex_using_timedelta(self):
        self.assertEqual(self.redis.setex('foo', timedelta(seconds=100), 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_setnx(self):
        self.assertEqual(self.redis.setnx('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'),  b'bar')
        self.assertEqual(self.redis.setnx('foo', 'baz'), False)
        self.assertEqual(self.redis.get('foo'),  b'bar')

    def test_delete(self):
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis.delete('foo'), True)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_delete_expire(self):
        self.redis.set("foo", "bar", ex=1)
        self.redis.delete("foo")
        self.redis.set("foo", "bar")
        sleep(2)
        self.assertEqual(self.redis.get("foo"), b'bar')

    def test_delete_multiple(self):
        self.redis['one'] = 'one'
        self.redis['two'] = 'two'
        self.redis['three'] = 'three'
        # Since redis>=2.7.6 returns number of deleted items.
        self.assertEqual(self.redis.delete('one', 'two'), 2)
        self.assertEqual(self.redis.get('one'), None)
        self.assertEqual(self.redis.get('two'), None)
        self.assertEqual(self.redis.get('three'), b'three')
        self.assertEqual(self.redis.delete('one', 'two'), False)
        # If any keys are deleted, True is returned.
        self.assertEqual(self.redis.delete('two', 'three'), True)
        self.assertEqual(self.redis.get('three'), None)

    def test_delete_nonexistent_key(self):
        self.assertEqual(self.redis.delete('foo'), False)

    ## Tests for the list type.

    def test_rpush_then_lrange_with_nested_list1(self):
        self.assertEqual(self.redis.rpush('foo', [long(12345), long(6789)]), 1)
        self.assertEqual(self.redis.rpush('foo', [long(54321), long(9876)]), 2)
        self.assertEqual(self.redis.lrange(
            'foo', 0, -1), ['[12345L, 6789L]', '[54321L, 9876L]'] if PY2 else
                           [b'[12345, 6789]', b'[54321, 9876]'])
        self.redis.flushall()

    def test_rpush_then_lrange_with_nested_list2(self):
        self.assertEqual(self.redis.rpush('foo', [long(12345), 'banana']), 1)
        self.assertEqual(self.redis.rpush('foo', [long(54321), 'elephant']), 2)
        self.assertEqual(self.redis.lrange(
            'foo', 0, -1), ['[12345L, \'banana\']', '[54321L, \'elephant\']'] if PY2 else
                           [b'[12345, \'banana\']', b'[54321, \'elephant\']'])
        self.redis.flushall()

    def test_rpush_then_lrange_with_nested_list3(self):
        self.assertEqual(self.redis.rpush('foo', [long(12345), []]), 1)
        self.assertEqual(self.redis.rpush('foo', [long(54321), []]), 2)

        self.assertEqual(self.redis.lrange(
            'foo', 0, -1), ['[12345L, []]', '[54321L, []]'] if PY2 else
                           [b'[12345, []]', b'[54321, []]'])
        self.redis.flushall()

    def test_lpush_then_lrange_all(self):
        self.assertEqual(self.redis.lpush('foo', 'bar'), 1)
        self.assertEqual(self.redis.lpush('foo', 'baz'), 2)
        self.assertEqual(self.redis.lpush('foo', 'bam', 'buzz'), 4)
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'buzz', b'bam', b'baz', b'bar'])

    def test_lpush_then_lrange_portion(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'three')
        self.redis.lpush('foo', 'four')
        self.assertEqual(self.redis.lrange('foo', 0, 2),
                         [b'four', b'three', b'two'])
        self.assertEqual(self.redis.lrange('foo', 0, 3),
                         [b'four', b'three', b'two', b'one'])

    def test_lpush_key_does_not_exist(self):
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_lpush_with_nonstr_key(self):
        self.redis.lpush(1, 'one')
        self.redis.lpush(1, 'two')
        self.redis.lpush(1, 'three')
        self.assertEqual(self.redis.lrange(1, 0, 2),
                         [b'three', b'two', b'one'])
        self.assertEqual(self.redis.lrange('1', 0, 2),
                         [b'three', b'two', b'one'])

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
        self.redis.lrem('foo', 2, 'same')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'different'])

    def test_lrem_negative_count(self):
        self.redis.lpush('foo', 'removeme')
        self.redis.lpush('foo', 'three')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'removeme')
        self.redis.lrem('foo', -1, 'removeme')
        # Should remove it from the end of the list,
        # leaving the 'removeme' from the front of the list alone.
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'removeme', b'one', b'two', b'three'])

    def test_lrem_zero_count(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 0, 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_lrem_default_value(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 0, 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [])

    def test_lrem_does_not_exist(self):
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 0, 'one')
        # These should be noops.
        self.redis.lrem('foo', -2, 'one')
        self.redis.lrem('foo', 2, 'one')

    def test_lrem_return_value(self):
        self.redis.lpush('foo', 'one')
        count = self.redis.lrem('foo', 0, 'one')
        self.assertEqual(count, 1)
        self.assertEqual(self.redis.lrem('foo', 0, 'one'), 0)

    def test_rpush(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.redis.rpush('foo', 'four', 'five')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'one', b'two', b'three', b'four', b'five'])

    def test_lpop(self):
        self.assertEqual(self.redis.rpush('foo', 'one'), 1)
        self.assertEqual(self.redis.rpush('foo', 'two'), 2)
        self.assertEqual(self.redis.rpush('foo', 'three'), 3)
        self.assertEqual(self.redis.lpop('foo'), b'one')
        self.assertEqual(self.redis.lpop('foo'), b'two')
        self.assertEqual(self.redis.lpop('foo'), b'three')

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
                         [b'four', b'five', b'three'])

    def test_lset_index_out_of_range(self):
        self.redis.rpush('foo', 'one')
        with self.assertRaises(redis.ResponseError):
            self.redis.lset('foo', 3, 'three')

    def test_rpushx(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpushx('foo', 'two')
        self.redis.rpushx('bar', 'three')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one', b'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_ltrim(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.redis.rpush('foo', 'four')

        self.assertTrue(self.redis.ltrim('foo', 1, 3))
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'two', b'three',
                                                           b'four'])
        self.assertTrue(self.redis.ltrim('foo', 1, -1))
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'three', b'four'])

    def test_ltrim_with_non_existent_key(self):
        self.assertTrue(self.redis.ltrim('foo', 0, -1))

    def test_lindex(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.lindex('foo', 0), b'one')
        self.assertEqual(self.redis.lindex('foo', 4), None)
        self.assertEqual(self.redis.lindex('bar', 4), None)

    def test_lpushx(self):
        self.redis.lpush('foo', 'two')
        self.redis.lpushx('foo', 'one')
        self.redis.lpushx('bar', 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one', b'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_rpop(self):
        self.assertEqual(self.redis.rpop('foo'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.rpop('foo'), b'two')
        self.assertEqual(self.redis.rpop('foo'), b'one')
        self.assertEqual(self.redis.rpop('foo'), None)

    def test_linsert(self):
        self.redis.rpush('foo', 'hello')
        self.redis.rpush('foo', 'world')
        self.redis.linsert('foo', 'before', 'world', 'there')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'hello', b'there', b'world'])

    def test_rpoplpush(self):
        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), None)
        self.assertEqual(self.redis.lpop('bar'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('bar', 'one')

        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), b'two')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [b'two', b'one'])

    def test_rpoplpush_to_nonexistent_destination(self):
        self.redis.rpush('foo', 'one')
        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), b'one')
        self.assertEqual(self.redis.rpop('bar'), b'one')

    def test_blpop_single_list(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.assertEqual(self.redis.blpop(['foo'], timeout=1),
                         (b'foo', b'one'))

    def test_blpop_test_multiple_lists(self):
        self.redis.rpush('baz', 'zero')
        self.assertEqual(self.redis.blpop(['foo', 'baz'], timeout=1),
                         (b'baz', b'zero'))

        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        # bar has nothing, so the returned value should come
        # from foo.
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         (b'foo', b'one'))
        self.redis.rpush('bar', 'three')
        # bar now has something, so the returned value should come
        # from bar.
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         (b'bar', b'three'))
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         (b'foo', b'two'))

    def test_blpop_allow_single_key(self):
        # blpop converts single key arguments to a one element list.
        self.redis.rpush('foo', 'one')
        self.assertEqual(self.redis.blpop('foo', timeout=1), (b'foo', b'one'))

    def test_brpop_test_multiple_lists(self):
        self.redis.rpush('baz', 'zero')
        self.assertEqual(self.redis.brpop(['foo', 'baz'], timeout=1),
                         (b'baz', b'zero'))

        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpop(['bar', 'foo'], timeout=1),
                         (b'foo', b'two'))

    def test_brpop_single_key(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpop('foo', timeout=1),
                         (b'foo', b'two'))

    def test_brpoplpush_multi_keys(self):
        self.assertEqual(self.redis.lpop('bar'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpoplpush('foo', 'bar', timeout=1),
                         b'two')
        self.assertEqual(self.redis.lrange('bar', 0, -1), [b'two'])

    @attr('slow')
    def test_blocking_operations_when_empty(self):
        self.assertEqual(self.redis.blpop(['foo'], timeout=1),
                         None)
        self.assertEqual(self.redis.blpop(['bar', 'foo'], timeout=1),
                         None)
        self.assertEqual(self.redis.brpop('foo', timeout=1),
                         None)
        self.assertEqual(self.redis.brpoplpush('foo', 'bar', timeout=1),
                         None)

    ## Tests for the hash type.

    def test_hset_then_hget(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        self.assertEqual(self.redis.hget('foo', 'key'), b'value')

    def test_hset_update(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 0)

    def test_hgetall(self):
        self.assertEqual(self.redis.hset('foo', 'k1', 'v1'), 1)
        self.assertEqual(self.redis.hset('foo', 'k2', 'v2'), 1)
        self.assertEqual(self.redis.hset('foo', 'k3', 'v3'), 1)
        self.assertEqual(self.redis.hgetall('foo'), {b'k1': b'v1',
                                                     b'k2': b'v2',
                                                     b'k3': b'v3'})

    def test_hgetall_with_tuples(self):
        self.assertEqual(self.redis.hset('foo', (1, 2), (1, 2, 3)), 1)
        self.assertEqual(self.redis.hgetall('foo'), {b'(1, 2)': b'(1, 2, 3)'})

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
        self.assertEqual(set(self.redis.hkeys('foo')), set([b'k1', b'k2']))
        self.assertEqual(set(self.redis.hkeys('bar')), set([]))

    def test_hlen(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(self.redis.hlen('foo'), 2)

    def test_hvals(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(set(self.redis.hvals('foo')), set([b'v1', b'v2']))
        self.assertEqual(set(self.redis.hvals('bar')), set([]))

    def test_hmget(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.redis.hset('foo', 'k3', 'v3')
        # Normal case.
        self.assertEqual(self.redis.hmget('foo', ['k1', 'k3']), [b'v1', b'v3'])
        self.assertEqual(self.redis.hmget('foo', 'k1', 'k3'), [b'v1', b'v3'])
        # Key does not exist.
        self.assertEqual(self.redis.hmget('bar', ['k1', 'k3']), [None, None])
        self.assertEqual(self.redis.hmget('bar', 'k1', 'k3'), [None, None])
        # Some keys in the hash do not exist.
        self.assertEqual(self.redis.hmget('foo', ['k1', 'k500']),
                         [b'v1', None])
        self.assertEqual(self.redis.hmget('foo', 'k1', 'k500'),
                         [b'v1', None])

    def test_hdel(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.redis.hset('foo', 'k3', 'v3')
        self.assertEqual(self.redis.hget('foo', 'k1'), b'v1')
        self.assertEqual(self.redis.hdel('foo', 'k1'), True)
        self.assertEqual(self.redis.hget('foo', 'k1'), None)
        self.assertEqual(self.redis.hdel('foo', 'k1'), False)
        # Since redis>=2.7.6 returns number of deleted items.
        self.assertEqual(self.redis.hdel('foo', 'k2', 'k3'), 2)
        self.assertEqual(self.redis.hget('foo', 'k2'), None)
        self.assertEqual(self.redis.hget('foo', 'k3'), None)
        self.assertEqual(self.redis.hdel('foo', 'k2', 'k3'), False)

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
        self.assertEqual(self.redis.hget('foo', 'newkey'), b'v1')

    def test_hmsetset_empty_raises_error(self):
        with self.assertRaises(redis.DataError):
            self.redis.hmset('foo', {})

    def test_hmsetset(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.assertEqual(self.redis.hmset('foo', {'k2': 'v2', 'k3': 'v3'}),
                         True)

    def test_hmset_convert_values(self):
        self.redis.hmset('foo', {'k1': True, 'k2': 1})
        self.assertEqual(self.redis.hgetall('foo'), {b'k1': b'True', b'k2': b'1'})

    def test_sadd(self):
        self.assertEqual(self.redis.sadd('foo', 'member1'), 1)
        self.assertEqual(self.redis.sadd('foo', 'member1'), 0)
        self.assertEqual(self.redis.smembers('foo'), set([b'member1']))
        self.assertEqual(self.redis.sadd('foo', 'member2', 'member3'), 2)
        self.assertEqual(self.redis.smembers('foo'),
                         set([b'member1', b'member2', b'member3']))
        self.assertEqual(self.redis.sadd('foo', 'member3', 'member4'), 1)
        self.assertEqual(self.redis.smembers('foo'),
                         set([b'member1', b'member2', b'member3', b'member4']))

    def test_sadd_as_str_type(self):
        self.assertEqual(self.redis.sadd('foo', *range(3)), 3)
        self.assertEqual(self.redis.smembers('foo'), set([b'0', b'1', b'2']))

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
        self.assertEqual(self.redis.sdiff('foo', 'bar'), set([b'member1']))
        # Original sets shouldn't be modified.
        self.assertEqual(self.redis.smembers('foo'),
                         set([b'member1', b'member2']))
        self.assertEqual(self.redis.smembers('bar'),
                         set([b'member2', b'member3']))

    def test_sdiff_one_key(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.sdiff('foo'),
                         set([b'member1', b'member2']))

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
        self.assertEqual(self.redis.sinter('foo', 'bar'), set([b'member2']))
        self.assertEqual(self.redis.sinter('foo'),
                         set([b'member1', b'member2']))

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

    def test_smembers(self):
        self.assertEqual(self.redis.smembers('foo'), set())

    def test_smove(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), True)
        self.assertEqual(self.redis.smembers('bar'), set([b'member1']))

    def test_smove_non_existent_key(self):
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), False)

    def test_spop(self):
        # This is tricky because it pops a random element.
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.spop('foo'), b'member1')
        self.assertEqual(self.redis.spop('foo'), None)

    def test_srandmember(self):
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.srandmember('foo'), b'member1')
        # Shouldn't be removed from the set.
        self.assertEqual(self.redis.srandmember('foo'), b'member1')

    def test_srem(self):
        self.redis.sadd('foo', 'member1', 'member2', 'member3', 'member4')
        self.assertEqual(self.redis.smembers('foo'),
                         set([b'member1', b'member2', b'member3', b'member4']))
        self.assertEqual(self.redis.srem('foo', 'member1'), True)
        self.assertEqual(self.redis.smembers('foo'),
                         set([b'member2', b'member3', b'member4']))
        self.assertEqual(self.redis.srem('foo', 'member1'), False)
        # Since redis>=2.7.6 returns number of deleted items.
        self.assertEqual(self.redis.srem('foo', 'member2', 'member3'), 2)
        self.assertEqual(self.redis.smembers('foo'), set([b'member4']))
        self.assertEqual(self.redis.srem('foo', 'member3', 'member4'), True)
        self.assertEqual(self.redis.smembers('foo'), set([]))
        self.assertEqual(self.redis.srem('foo', 'member3', 'member4'), False)

    def test_sunion(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunion('foo', 'bar'),
                         set([b'member1', b'member2', b'member3']))

    def test_sunionstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunionstore('baz', 'foo', 'bar'), 3)
        self.assertEqual(self.redis.smembers('baz'),
                         set([b'member1', b'member2', b'member3']))

    def test_zadd(self):
        self.redis.zadd('foo', four=4)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zadd('foo', 2, 'two', 1, 'one', zero=0), 3)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         [b'zero', b'one', b'two', b'three', b'four'])
        self.assertEqual(self.redis.zadd('foo', 7, 'zero', one=1, five=5), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         [b'one', b'two', b'three', b'four', b'five', b'zero'])

    def test_zadd_uses_str(self):
        self.redis.zadd('foo', 12345, (1, 2, 3))
        self.assertEqual(self.redis.zrange('foo', 0, 0), [b'(1, 2, 3)'])

    def test_zadd_errors(self):
        # The args are backwards, it should be 2, "two", so we
        # expect an exception to be raised.
        with self.assertRaises(redis.ResponseError):
            self.redis.zadd('foo', 'two', 2)
        with self.assertRaises(redis.ResponseError):
            self.redis.zadd('foo', two='two')
        # It's expected an equal number of values and scores
        with self.assertRaises(redis.RedisError):
            self.redis.zadd('foo', 'two')

    def test_zadd_multiple(self):
        self.redis.zadd('foo', 1, 'one', 2, 'two')
        self.assertEqual(self.redis.zrange('foo', 0, 0),
                         [b'one'])
        self.assertEqual(self.redis.zrange('foo', 1, 1),
                         [b'two'])

    def test_zrange_same_score(self):
        self.redis.zadd('foo', two_a=2)
        self.redis.zadd('foo', two_b=2)
        self.redis.zadd('foo', two_c=2)
        self.redis.zadd('foo', two_d=2)
        self.redis.zadd('foo', two_e=2)
        self.assertEqual(self.redis.zrange('foo', 2, 3),
                         [b'two_c', b'two_d'])

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
        self.assertEqual(self.redis.zcount('foo', 4, '+inf'), 1)
        self.assertEqual(self.redis.zcount('foo', '-inf', 4), 2)
        self.assertEqual(self.redis.zcount('foo', '-inf', '+inf'), 3)

    def test_zcount_exclusive(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', three=2)
        self.redis.zadd('foo', five=5)
        self.assertEqual(self.redis.zcount('foo', '-inf', '(2'), 1)
        self.assertEqual(self.redis.zcount('foo', '-inf', 2), 2)
        self.assertEqual(self.redis.zcount('foo', '(5', '+inf'), 0)
        self.assertEqual(self.redis.zcount('foo', '(1', 5), 2)
        self.assertEqual(self.redis.zcount('foo', '(2', '(5'), 0)
        self.assertEqual(self.redis.zcount('foo', '(1', '(5'), 1)
        self.assertEqual(self.redis.zcount('foo', 2, '(5'), 1)

    def test_zincrby(self):
        self.redis.zadd('foo', one=1)
        self.assertEqual(self.redis.zincrby('foo', 'one', 10), 11)
        self.assertEqual(self.redis.zrange('foo', 0, -1, withscores=True),
                         [(b'one', 11)])

    def test_zrange_descending(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, -1, desc=True),
                         [b'three', b'two', b'one'])

    def test_zrange_descending_with_scores(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, -1, desc=True,
                                           withscores=True),
                         [(b'three', 3), (b'two', 2), (b'one', 1)])

    def test_zrange_with_positive_indices(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrange('foo', 0, 1), [b'one', b'two'])

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
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.redis.zadd('foo', four=4)
        self.assertEqual(self.redis.zrem('foo', 'one'), True)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         [b'two', b'three', b'four'])
        # Since redis>=2.7.6 returns number of deleted items.
        self.assertEqual(self.redis.zrem('foo', 'two', 'three'), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'four'])
        self.assertEqual(self.redis.zrem('foo', 'three', 'four'), True)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [])
        self.assertEqual(self.redis.zrem('foo', 'three', 'four'), False)

    def test_zrem_non_existent_member(self):
        self.assertFalse(self.redis.zrem('foo', 'one'))

    def test_zrem_numeric_member(self):
        self.redis.zadd('foo', **{'128': 13.0, '129': 12.0})
        self.assertEqual(self.redis.zrem('foo',  128), True)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'129'])

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
        self.assertEqual(self.redis.zrevrange('foo', 0, 1), [b'three', b'two'])
        self.assertEqual(self.redis.zrevrange('foo', 0, -1),
                         [b'three', b'two', b'one'])

    def test_zrevrange_sorted_keys(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', 2, 'two_b')
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrange('foo', 0, 2), [b'three', b'two_b', b'two'])
        self.assertEqual(self.redis.zrevrange('foo', 0, -1),
                         [b'three', b'two_b', b'two', b'one'])

    def test_zrangebyscore(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', two_a_also=2)
        self.redis.zadd('foo', two_b_also=2)
        self.redis.zadd('foo', four=4)
        self.assertEqual(self.redis.zrangebyscore('foo', 1, 3),
                         [b'two', b'two_a_also', b'two_b_also'])
        self.assertEqual(self.redis.zrangebyscore('foo', 2, 3),
                         [b'two', b'two_a_also', b'two_b_also'])
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4),
                         [b'zero', b'two', b'two_a_also', b'two_b_also',
                          b'four'])
        self.assertEqual(self.redis.zrangebyscore('foo', '-inf', 1),
                         [b'zero'])
        self.assertEqual(self.redis.zrangebyscore('foo', 2, '+inf'),
                         [b'two', b'two_a_also', b'two_b_also', b'four'])
        self.assertEqual(self.redis.zrangebyscore('foo', '-inf', '+inf'),
                         [b'zero', b'two', b'two_a_also', b'two_b_also',
                          b'four'])

    def test_zrangebysore_exclusive(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', four=4)
        self.redis.zadd('foo', five=5)
        self.assertEqual(self.redis.zrangebyscore('foo', '(0', 6),
                         [b'two', b'four', b'five'])
        self.assertEqual(self.redis.zrangebyscore('foo', '(2', '(5'),
                         [b'four'])
        self.assertEqual(self.redis.zrangebyscore('foo', 0, '(4'),
                         [b'zero', b'two'])

    def test_zrangebyscore_raises_error(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebyscore('foo', 'one', 2)
        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebyscore('foo', 2, 'three')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebyscore('foo', 2, '3)')
        with self.assertRaises(redis.RedisError):
            self.redis.zrangebyscore('foo', 2, '3)', 0, None)

    def test_zrangebyscore_slice(self):
        self.redis.zadd('foo', two_a=2)
        self.redis.zadd('foo', two_b=2)
        self.redis.zadd('foo', two_c=2)
        self.redis.zadd('foo', two_d=2)
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4, 0, 2),
                         [b'two_a', b'two_b'])
        self.assertEqual(self.redis.zrangebyscore('foo', 0, 4, 1, 3),
                         [b'two_b', b'two_c', b'two_d'])

    def test_zrangebyscore_withscores(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrangebyscore('foo', 1, 3, 0, 2, True),
                         [(b'one', 1), (b'two', 2)])

    def test_zrevrangebyscore(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1),
                         [b'three', b'two', b'one'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 2),
                         [b'three', b'two'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1, 0, 1),
                         [b'three'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, 1, 1, 2),
                         [b'two', b'one'])

    def test_zrevrangebyscore_exclusive(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrangebyscore('foo', '(3', 1),
                         [b'two', b'one'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', 3, '(2'),
                         [b'three'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', '(3', '(1'),
                         [b'two'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', '(2', 1, 0, 1),
                         [b'one'])
        self.assertEqual(self.redis.zrevrangebyscore('foo', '(2', '(1', 0, 1),
                         [])
        self.assertEqual(self.redis.zrevrangebyscore('foo', '(3', '(0', 1, 2),
                         [b'one'])

    def test_zrevrangebyscore_raises_error(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebyscore('foo', 'three', 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebyscore('foo', 3, 'one')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebyscore('foo', 3, '1)')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebyscore('foo', '((3', '1)')

    def test_zremrangebyrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zremrangebyrank('foo', 0, 1), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'three'])

    def test_zremrangebyrank_negative_indices(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zremrangebyrank('foo', -2, -1), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'one'])

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
                         [b'zero', b'two', b'four'])
        # Middle of range.
        self.assertEqual(self.redis.zremrangebyscore('foo', 1, 3), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'zero', b'four'])
        self.assertEqual(self.redis.zremrangebyscore('foo', 1, 3), 0)
        # Entire range.
        self.assertEqual(self.redis.zremrangebyscore('foo', 0, 4), 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [])

    def test_zremrangebyscore_exclusive(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', four=4)
        self.assertEqual(self.redis.zremrangebyscore('foo', '(0', 1), 0)
        self.assertEqual(self.redis.zrange('foo', 0, -1),
                         [b'zero', b'two', b'four'])
        self.assertEqual(self.redis.zremrangebyscore('foo', '-inf', '(0'), 0)
        self.assertEqual(self.redis.zrange('foo', 0, -1), 
                         [b'zero', b'two', b'four'])
        self.assertEqual(self.redis.zremrangebyscore('foo', '(2', 5), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'zero', b'two'])
        self.assertEqual(self.redis.zremrangebyscore('foo', 0, '(2'), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'two'])
        self.assertEqual(self.redis.zremrangebyscore('foo', '(1', '(3'), 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [])

    def test_zremrangebyscore_raises_error(self):
        self.redis.zadd('foo', zero=0)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', four=4)
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyscore('foo', 'three', 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyscore('foo', 3, 'one')
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyscore('foo', 3, '1)')
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyscore('foo', '((3', '1)')

    def test_zremrangebyscore_badkey(self):
        self.assertEqual(self.redis.zremrangebyscore('foo', 0, 2), 0)

    def test_zunionstore(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'])
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 2), (b'three', 3), (b'two', 4)])

    def test_zunionstore_sum(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 2), (b'three', 3), (b'two', 4)])

    def test_zunionstore_max(self):
        self.redis.zadd('foo', one=0)
        self.redis.zadd('foo', two=0)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 1), (b'two', 2), (b'three', 3)])

    def test_zunionstore_min(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=0)
        self.redis.zadd('bar', two=0)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='MIN')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 0), (b'two', 0), (b'three', 3)])

    def test_zunionstore_weights(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', four=4)
        self.redis.zunionstore('baz', {'foo': 1, 'bar': 2}, aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 3), (b'two', 6), (b'four', 8)])

    def test_zunionstore_mixed_set_types(self):
        # No score, redis will use 1.0.
        self.redis.sadd('foo', 'one')
        self.redis.sadd('foo', 'two')
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 2), (b'three', 3), (b'two', 3)])

    def test_zunionstore_badkey(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zunionstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 1), (b'two', 2)])
        self.redis.zunionstore('baz', {'foo': 1, 'bar': 2}, aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 1), (b'two', 2)])

    def test_zinterstore(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zinterstore('baz', ['foo', 'bar'])
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 2), (b'two', 4)])

    def test_zinterstore_mixed_set_types(self):
        self.redis.sadd('foo', 'one')
        self.redis.sadd('foo', 'two')
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zinterstore('baz', ['foo', 'bar'], aggregate='SUM')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 2), (b'two', 3)])

    def test_zinterstore_max(self):
        self.redis.zadd('foo', one=0)
        self.redis.zadd('foo', two=0)
        self.redis.zadd('bar', one=1)
        self.redis.zadd('bar', two=2)
        self.redis.zadd('bar', three=3)
        self.redis.zinterstore('baz', ['foo', 'bar'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 1), (b'two', 2)])

    def test_zinterstore_onekey(self):
        self.redis.zadd('foo', one=1)
        self.redis.zinterstore('baz', ['foo'], aggregate='MAX')
        self.assertEqual(self.redis.zrange('baz', 0, -1, withscores=True),
                         [(b'one', 1)])

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

        self.assertEqual(r1['r1'], b'r1')
        self.assertEqual(r2['r2'], b'r2')

        r1.flushall()

        self.assertTrue('r1' not in r1)
        self.assertTrue('r2' not in r2)

    def test_basic_sort(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo'), [b'1', b'2', b'3'])

    def test_empty_sort(self):
        self.assertEqual(self.redis.sort('foo'), [])

    def test_sort_range_offset_range(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo', start=0, num=2), [b'1', b'2'])

    def test_sort_range_offset_range_and_desc(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort("foo", start=0, num=1, desc=True), [b"4"])

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
                         [b'2', b'3', b'4'])

    def test_sort_descending(self):
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '3')
        self.assertEqual(self.redis.sort('foo', desc=True), [b'3', b'2', b'1'])

    def test_sort_alpha(self):
        self.redis.rpush('foo', '2a')
        self.redis.rpush('foo', '1b')
        self.redis.rpush('foo', '2b')
        self.redis.rpush('foo', '1a')

        self.assertEqual(self.redis.sort('foo', alpha=True),
                         [b'1a', b'1b', b'2a', b'2b'])

    def test_foo(self):
        self.redis.rpush('foo', '2a')
        self.redis.rpush('foo', '1b')
        self.redis.rpush('foo', '2b')
        self.redis.rpush('foo', '1a')
        with self.assertRaises(redis.ResponseError):
            self.redis.sort('foo', alpha=False)

    def test_sort_with_store_option(self):
        self.redis.rpush('foo', '2')
        self.redis.rpush('foo', '1')
        self.redis.rpush('foo', '4')
        self.redis.rpush('foo', '3')

        self.assertEqual(self.redis.sort('foo', store='bar'), 4)
        self.assertEqual(self.redis.lrange('bar', 0, -1),
                         [b'1', b'2', b'3', b'4'])

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
                         [b'four', b'three', b'two', b'one'])
        self.assertEqual(self.redis.sort('foo', by='weight_*', get='#'),
                         [b'4', b'3', b'2', b'1'])
        self.assertEqual(
            self.redis.sort('foo', by='weight_*', get=('data_*', '#')),
            [b'four', b'4', b'three', b'3', b'two', b'2', b'one', b'1'])
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
                         [b'youngest', b'middle', b'eldest'])
        self.assertEqual(
            self.redis.sort('foo', by='record_*->age', get='record_*->name'),
            [b'baby', b'teen', b'adult'])

    def test_sort_with_set(self):
        self.redis.sadd('foo', '3')
        self.redis.sadd('foo', '1')
        self.redis.sadd('foo', '2')
        self.assertEqual(self.redis.sort('foo'), [b'1', b'2', b'3'])

    def test_pipeline(self):
        # The pipeline method returns an object for
        # issuing multiple commands in a batch.
        p = self.redis.pipeline()
        p.watch('bam')
        p.multi()
        p.set('foo', 'bar').get('foo')
        p.lpush('baz', 'quux')
        p.lpush('baz', 'quux2').lrange('baz', 0, -1)
        res = p.execute()

        # Check return values returned as list.
        self.assertEqual([True, b'bar', 1, 2, [b'quux2', b'quux']], res)

        # Check side effects happened as expected.
        self.assertEqual([b'quux2', b'quux'], self.redis.lrange('baz', 0, -1))

        # Check that the command buffer has been emptied.
        self.assertEqual([], p.execute())

    def test_multiple_successful_watch_calls(self):
        p = self.redis.pipeline()
        p.watch('bam')
        p.multi()
        p.set('foo', 'bar')
        # Check that the watched keys buffer has been emptied.
        p.execute()

        # bam is no longer being watched, so it's ok to modify
        # it now.
        p.watch('foo')
        self.redis.set('bam', 'boo')
        p.multi()
        p.set('foo', 'bats')
        self.assertEqual(p.execute(), [True])

    def test_pipeline_non_transactional(self):
        # For our simple-minded model I don't think
        # there is any observable difference.
        p = self.redis.pipeline(transaction=False)
        res = p.set('baz', 'quux').get('baz').execute()

        self.assertEqual([True, b'quux'], res)

    def test_pipeline_raises_when_watched_key_changed(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        self.addCleanup(p.reset)

        p.watch('greet', 'foo')
        nextf = p.get('foo') + b'baz'
        # Simulate change happening on another thread.
        self.redis.rpush('greet', 'world')
        # Begin pipelining.
        p.multi()
        p.set('foo', nextf)

        self.assertRaises(redis.WatchError, p.execute)

    def test_pipeline_succeeds_despite_unwatched_key_changed(self):
        # Same setup as before except for the params to the WATCH command.
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            # Only watch one of the 2 keys.
            p.watch('foo')
            nextf = p.get('foo') + b'baz'
            # Simulate change happening on another thread.
            self.redis.rpush('greet', 'world')
            p.multi()
            p.set('foo', nextf)
            p.execute()

            # Check the commands were executed.
            self.assertEqual(b'barbaz', self.redis.get('foo'))
        finally:
            p.reset()

    def test_pipeline_succeeds_when_watching_nonexistent_key(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            # Also watch a nonexistent key.
            p.watch('foo', 'bam')
            nextf = p.get('foo') + b'baz'
            # Simulate change happening on another thread.
            self.redis.rpush('greet', 'world')
            p.multi()
            p.set('foo', nextf)
            p.execute()

            # Check the commands were executed.
            self.assertEqual(b'barbaz', self.redis.get('foo'))
        finally:
            p.reset()

    def test_watch_state_is_cleared_across_multiple_watches(self):
        self.redis.set('foo', 'one')
        self.redis.set('bar', 'baz')
        p = self.redis.pipeline()
        self.addCleanup(p.reset)

        p.watch('foo')
        # Simulate change happening on another thread.
        self.redis.set('foo', 'three')
        p.multi()
        p.set('foo', 'three')
        with self.assertRaises(redis.WatchError):
            p.execute()

        # Now watch another key.  It should be ok to change
        # foo as we're no longer watching it.
        p.watch('bar')
        self.redis.set('foo', 'four')
        p.multi()
        p.set('bar', 'five')
        self.assertEqual(p.execute(), [True])

    def test_pipeline_proxies_to_redis_object(self):
        p = self.redis.pipeline()
        self.assertTrue(hasattr(p, 'zadd'))
        with self.assertRaises(AttributeError):
            p.non_existent_attribute

    def test_pipeline_as_context_manager(self):
        self.redis.set('foo', 'bar')
        with self.redis.pipeline() as p:
            p.watch('foo')
            self.assertTrue(isinstance(p, redis.client.BasePipeline)
                            or p.need_reset)
            p.multi()
            p.set('foo', 'baz')
            p.execute()

        # Usually you would consider the pipeline to
        # have been destroyed
        # after the with statement, but we need to check
        # it was reset properly:
        self.assertTrue(isinstance(p, redis.client.BasePipeline)
                        or not p.need_reset)

    def test_pipeline_transaction_shortcut(self):
        # This example taken pretty much from the redis-py documentation.
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

    def test_key_patterns(self):
        self.redis.mset({'one': 1, 'two': 2, 'three': 3, 'four': 4})
        self.assertItemsEqual(self.redis.keys('*o*'),
                              [b'four', b'one', b'two'])
        self.assertItemsEqual(self.redis.keys('t??'), [b'two'])
        self.assertItemsEqual(self.redis.keys('*'),
                              [b'four', b'one', b'two', b'three'])
        self.assertItemsEqual(self.redis.keys(),
                              [b'four', b'one', b'two', b'three'])

    def test_ping(self):
        self.assertTrue(self.redis.ping())


class TestFakeRedis(unittest.TestCase):
    def setUp(self):
        self.redis = self.create_redis()

    def tearDown(self):
        self.redis.flushall()
        del self.redis

    def assertInRange(self, value, start, end, msg=None):
        self.assertGreaterEqual(value, start, msg)
        self.assertLessEqual(value, end, msg)

    def create_redis(self, db=0):
        return fakeredis.FakeRedis(db=db)

    def test_setex(self):
        self.assertEqual(self.redis.setex('foo', 'bar', 100), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_setex_using_timedelta(self):
        self.assertEqual(self.redis.setex('foo', 'bar', timedelta(seconds=100)), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_lrem_postitive_count(self):
        self.redis.lpush('foo', 'same')
        self.redis.lpush('foo', 'same')
        self.redis.lpush('foo', 'different')
        self.redis.lrem('foo', 'same', 2)
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'different'])

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
                         [b'removeme', b'one', b'two', b'three'])

    def test_lrem_zero_count(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'one')
        self.redis.lrem('foo', 'one')
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
        count = self.redis.lrem('foo', 'one', 0)
        self.assertEqual(count, 1)
        self.assertEqual(self.redis.lrem('foo', 'one'), 0)

    def test_zadd_deprecated(self):
        self.redis.zadd('foo', 'one', 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'one'])

    def test_zadd_missing_required_params(self):
        with self.assertRaises(redis.RedisError):
            # Missing the 'score' param.
            self.redis.zadd('foo', 'one')
        with self.assertRaises(redis.RedisError):
            # Missing the 'value' param.
            self.redis.zadd('foo', None, score=1)

    def test_zadd_with_single_keypair(self):
        self.redis.zadd('foo', bar=1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'bar'])

    def test_set_nx_doesnt_set_value_twice(self):
        self.assertEqual(self.redis.set('foo', 'bar', nx=True), True)
        self.assertEqual(self.redis.set('foo', 'bar', nx=True), None)

    def test_set_xx_set_value_when_exists(self):
        self.assertEqual(self.redis.set('foo', 'bar', xx=True), None)
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.set('foo', 'bar', xx=True), True)

    @attr('slow')
    def test_set_ex_should_expire_value(self):
        self.redis.set('foo', 'bar', ex=0)
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.set('foo', 'bar', ex=1)
        sleep(2)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_set_px_should_expire_value(self):
        self.redis.set('foo', 'bar', px=500)
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_psetex_expire_value(self):
        self.assertRaises(ResponseError, self.redis.psetex, 'foo', 0, 'bar')
        self.redis.psetex('foo', 500, 'bar')
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_psetex_expire_value_using_timedelta(self):
        self.assertRaises(ResponseError, self.redis.psetex, 'foo', timedelta(seconds=0), 'bar')
        self.redis.psetex('foo', timedelta(seconds=0.5), 'bar')
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_expire_should_expire_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expire('foo', 1)
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    @attr('slow')
    def test_expire_should_expire_key_using_timedelta(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expire('foo', timedelta(seconds=1))
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    @attr('slow')
    def test_expireat_should_expire_key_by_datetime(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expireat('foo', datetime.now() + timedelta(seconds=1))
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expireat('bar', datetime.now()), False)

    @attr('slow')
    def test_expireat_should_expire_key_by_timestamp(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expireat('foo', int(time() + 1))
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    def test_ttl_should_return_none_for_non_expiring_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.ttl('foo'), None)

    def test_ttl_should_return_value_for_expiring_key(self):
        self.redis.set('foo', 'bar')
        self.redis.expire('foo', 1)
        self.assertEqual(self.redis.ttl('foo'), 1)
        self.redis.expire('foo', 2)
        self.assertEqual(self.redis.ttl('foo'), 2)
        long_long_c_max = 100000000000
        # See https://github.com/antirez/redis/blob/unstable/src/db.c#L632
        self.redis.expire('foo', long_long_c_max)
        self.assertEqual(self.redis.ttl('foo'), long_long_c_max)

    def test_pttl_should_return_none_for_non_expiring_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.pttl('foo'), None)

    def test_pttl_should_return_value_for_expiring_key(self):
        d = 100
        self.redis.set('foo', 'bar')
        self.redis.expire('foo', 1)
        self.assertInRange(self.redis.pttl('foo'), 1000 - d, 1000)
        self.redis.expire('foo', 2)
        self.assertInRange(self.redis.pttl('foo'), 2000 - d, 2000)
        long_long_c_max = 100000000000
        # See https://github.com/antirez/redis/blob/unstable/src/db.c#L632
        self.redis.expire('foo', long_long_c_max)
        self.assertInRange(self.redis.pttl('foo'),
                           long_long_c_max * 1000 - d,
                           long_long_c_max * 1000)


@redis_must_be_running
class TestRealRedis(TestFakeRedis):
    def create_redis(self, db=0):
        return redis.Redis('localhost', port=6379, db=db)


@redis_must_be_running
class TestRealStrictRedis(TestFakeStrictRedis):
    def create_redis(self, db=0):
        return redis.StrictRedis('localhost', port=6379, db=db)


class TestInitArgs(unittest.TestCase):
    def test_can_accept_any_kwargs(self):
        fakeredis.FakeRedis(foo='bar', bar='baz')
        fakeredis.FakeStrictRedis(foo='bar', bar='baz')

    def test_from_url(self):
        db = fakeredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/0')
        db.set('foo', 'bar')
        self.assertEqual(db.get('foo'), b'bar')

    def test_from_url_with_db_arg(self):
        db = fakeredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/0')
        db1 = fakeredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/1')
        db2 = fakeredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/',
            db=2)
        db.set('foo', 'foo0')
        db1.set('foo', 'foo1')
        db2.set('foo', 'foo2')
        self.assertEqual(db.get('foo'), b'foo0')
        self.assertEqual(db1.get('foo'), b'foo1')
        self.assertEqual(db2.get('foo'), b'foo2')

    def test_from_url_db_value_error(self):
        # In ValueError, should default to 0
        db = fakeredis.FakeStrictRedis.from_url(
           'redis://username:password@localhost:6379/a')
        self.assertEqual(db._db_num, 0)


if __name__ == '__main__':
    unittest.main()
