#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep, time
from redis.exceptions import ResponseError
import inspect
from functools import wraps
import os
import sys
import threading

from nose.plugins.skip import SkipTest
from nose.plugins.attrib import attr
import redis
import redis.client

import fakenewsredis
from datetime import datetime, timedelta

try:
    # Python 2.6, 2.7
    from Queue import Queue
except:
    # Python 3
    from queue import Queue

PY2 = sys.version_info[0] == 2

if not PY2:
    long = int

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

# Try importlib, then imp, then the old builtin `reload`
try:
    from importlib import reload
except:
    try:
        from imp import reload
    except:
        pass


DEFAULT_ENCODING = fakenewsredis.DEFAULT_ENCODING


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
        for name, attribute in inspect.getmembers(cls):
            if name.startswith('test_'):
                @wraps(attribute)
                def skip_test(*args, **kwargs):
                    raise SkipTest("Redis is not running.")
                setattr(cls, name, skip_test)
        cls.setUp = lambda x: None
        cls.tearDown = lambda x: None
    return cls


def key_val_dict(size=100):
    return dict([(b'key:' + bytes([i]), b'val:' + bytes([i]))
                 for i in range(size)])


class TestFakeStrictRedis(unittest.TestCase):
    decode_responses = False

    def setUp(self):
        self.redis = self.create_redis()

    def tearDown(self):
        self.redis.flushall()
        del self.redis

    if sys.version_info >= (3,):
        def assertItemsEqual(self, a, b):
            return self.assertCountEqual(a, b)

    def create_redis(self, db=0):
        return fakenewsredis.FakeStrictRedis(db=db)

    def test_flushdb(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.keys(), [b'foo'])
        self.assertEqual(self.redis.flushdb(), True)
        self.assertEqual(self.redis.keys(), [])

    def test_set_then_get(self):
        self.assertEqual(self.redis.set('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_set_None_value(self):
        self.assertEqual(self.redis.set('foo', None), True)
        self.assertEqual(self.redis.get('foo'), b'None')

    def test_set_float_value(self):
        x = 1.23456789123456789
        self.redis.set('foo', x)
        self.assertEqual(float(self.redis.get('foo')), x)

    def test_saving_non_ascii_chars_as_value(self):
        self.assertEqual(self.redis.set('foo', 'Ñandu'), True)
        self.assertEqual(self.redis.get('foo'),
                         u'Ñandu'.encode(DEFAULT_ENCODING))

    def test_saving_unicode_type_as_value(self):
        self.assertEqual(self.redis.set('foo', u'Ñandu'), True)
        self.assertEqual(self.redis.get('foo'),
                         u'Ñandu'.encode(DEFAULT_ENCODING))

    def test_saving_non_ascii_chars_as_key(self):
        self.assertEqual(self.redis.set('Ñandu', 'foo'), True)
        self.assertEqual(self.redis.get('Ñandu'), b'foo')

    def test_saving_unicode_type_as_key(self):
        self.assertEqual(self.redis.set(u'Ñandu', 'foo'), True)
        self.assertEqual(self.redis.get(u'Ñandu'), b'foo')

    def test_get_does_not_exist(self):
        self.assertEqual(self.redis.get('foo'), None)

    def test_get_with_non_str_keys(self):
        self.assertEqual(self.redis.set('2', 'bar'), True)
        self.assertEqual(self.redis.get(2), b'bar')

    def test_get_invalid_type(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.get('foo')

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

    def test_getbit_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.getbit('foo', 1)

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

    def test_setbit_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.setbit('foo', 0, 1)

    def test_setbit_expiry(self):
        self.redis.set('foo', b'0x00', ex=10)
        self.redis.setbit('foo', 1, 1)
        self.assertGreater(self.redis.ttl('foo'), 0)

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

    def test_bitcount_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.bitcount('foo')

    def test_getset_not_exist(self):
        val = self.redis.getset('foo', 'bar')
        self.assertEqual(val, None)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_getset_exists(self):
        self.redis.set('foo', 'bar')
        val = self.redis.getset('foo', b'baz')
        self.assertEqual(val, b'bar')
        val = self.redis.getset('foo', b'baz2')
        self.assertEqual(val, b'baz')

    def test_getset_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.getset('foo', 'bar')

    def test_setitem_getitem(self):
        self.assertEqual(self.redis.keys(), [])
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis['foo'], b'bar')

    def test_strlen(self):
        self.redis['foo'] = 'bar'

        self.assertEqual(self.redis.strlen('foo'), 3)
        self.assertEqual(self.redis.strlen('noexists'), 0)

    def test_strlen_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.strlen('foo')

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

    def test_substr_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.substr('foo', 0)

    def test_append(self):
        self.assertTrue(self.redis.set('foo', 'bar'))
        self.assertEqual(self.redis.append('foo', 'baz'), 6)
        self.assertEqual(self.redis.get('foo'), b'barbaz')

    def test_append_with_no_preexisting_key(self):
        self.assertEqual(self.redis.append('foo', 'bar'), 3)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_append_wrong_type(self):
        self.redis.rpush('foo', b'x')
        with self.assertRaises(redis.ResponseError):
            self.redis.append('foo', b'x')

    def test_incr_with_no_preexisting_key(self):
        self.assertEqual(self.redis.incr('foo'), 1)
        self.assertEqual(self.redis.incr('bar', 2), 2)

    def test_incr_by(self):
        self.assertEqual(self.redis.incrby('foo'), 1)
        self.assertEqual(self.redis.incrby('bar', 2), 2)

    def test_incr_preexisting_key(self):
        self.redis.set('foo', 15)
        self.assertEqual(self.redis.incr('foo', 5), 20)
        self.assertEqual(self.redis.get('foo'), b'20')

    def test_incr_expiry(self):
        self.redis.set('foo', 15, ex=10)
        self.redis.incr('foo', 5)
        self.assertGreater(self.redis.ttl('foo'), 0)

    def test_incr_bad_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.incr('foo', 15)
        self.redis.rpush('foo2', 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.incr('foo2', 15)

    def test_incr_with_float(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.incr('foo', 2.0)

    def test_incr_followed_by_mget(self):
        self.redis.set('foo', 15)
        self.assertEqual(self.redis.incr('foo', 5), 20)
        self.assertEqual(self.redis.get('foo'), b'20')

    def test_incr_followed_by_mget_returns_strings(self):
        self.redis.incr('foo', 1)
        self.assertEqual(self.redis.mget(['foo']), [b'1'])

    def test_incrbyfloat(self):
        self.redis.set('foo', 0)
        self.assertEqual(self.redis.incrbyfloat('foo', 1.0), 1.0)
        self.assertEqual(self.redis.incrbyfloat('foo', 1.0), 2.0)

    def test_incrbyfloat_with_noexist(self):
        self.assertEqual(self.redis.incrbyfloat('foo', 1.0), 1.0)
        self.assertEqual(self.redis.incrbyfloat('foo', 1.0), 2.0)

    def test_incrbyfloat_expiry(self):
        self.redis.set('foo', 1.5, ex=10)
        self.redis.incrbyfloat('foo', 2.5)
        self.assertGreater(self.redis.ttl('foo'), 0)

    def test_incrbyfloat_bad_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaisesRegexp(redis.ResponseError, 'not a valid float'):
            self.redis.incrbyfloat('foo', 1.0)
        self.redis.rpush('foo2', 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.incrbyfloat('foo2', 1.0)

    def test_incrbyfloat_precision(self):
        x = 1.23456789123456789
        self.assertEqual(self.redis.incrbyfloat('foo', x), x)
        self.assertEqual(float(self.redis.get('foo')), x)

    def test_decr(self):
        self.redis.set('foo', 10)
        self.assertEqual(self.redis.decr('foo'), 9)
        self.assertEqual(self.redis.get('foo'), b'9')

    def test_decr_newkey(self):
        self.redis.decr('foo')
        self.assertEqual(self.redis.get('foo'), b'-1')

    def test_decr_expiry(self):
        self.redis.set('foo', 10, ex=10)
        self.redis.decr('foo', 5)
        self.assertGreater(self.redis.ttl('foo'), 0)

    def test_decr_badtype(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.decr('foo', 15)
        self.redis.rpush('foo2', 1)
        with self.assertRaises(redis.ResponseError):
            self.redis.decr('foo2', 15)

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

    def test_rename_expiry(self):
        self.redis.set('foo', 'value1', ex=10)
        self.redis.set('bar', 'value2')
        self.redis.rename('foo', 'bar')
        self.assertGreater(self.redis.ttl('bar'), 0)

    def test_mget(self):
        self.redis.set('foo', 'one')
        self.redis.set('bar', 'two')
        self.assertEqual(self.redis.mget(['foo', 'bar']), [b'one', b'two'])
        self.assertEqual(self.redis.mget(['foo', 'bar', 'baz']),
                         [b'one', b'two', None])
        self.assertEqual(self.redis.mget('foo', 'bar'), [b'one', b'two'])
        self.assertEqual(self.redis.mget('foo', 'bar', None),
                         [b'one', b'two', None])

    def test_mget_with_no_keys_raises_error(self):
        with self.assertRaisesRegexp(
                redis.ResponseError, 'wrong number of arguments'):
            self.redis.mget([])

    def test_mget_mixed_types(self):
        self.redis.hset('hash', 'bar', 'baz')
        self.redis.zadd('zset', 1, 'bar')
        self.redis.sadd('set', 'member')
        self.redis.rpush('list', 'item1')
        self.redis.set('string', 'value')
        self.assertEqual(
            self.redis.mget(['hash', 'zset', 'set', 'string', 'absent']),
            [None, None, None, b'value', None])

    def test_mset_with_no_keys_raises_error(self):
        with self.assertRaisesRegexp(
                redis.RedisError, 'MSET requires'):
            self.redis.mset([])

    def test_mset(self):
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mset({'foo': 'one', 'bar': 'two'}), True)
        self.assertEqual(self.redis.mget('foo', 'bar'), [b'one', b'two'])

    def test_mset_accepts_kwargs(self):
        self.assertEqual(
            self.redis.mset(foo='one', bar='two'), True)
        self.assertEqual(
            self.redis.mset(foo='one', baz='three'), True)
        self.assertEqual(self.redis.mget('foo', 'bar', 'baz'),
                         [b'one', b'two', b'three'])

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
        self.assertEqual(
            self.redis.setex('foo', timedelta(seconds=100), 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_setex_using_float(self):
        self.assertRaisesRegexp(
            redis.ResponseError, 'integer', self.redis.setex, 'foo', 1.2,
            'bar')

    def test_set_ex(self):
        self.assertEqual(self.redis.set('foo', 'bar', ex=100), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_set_ex_using_timedelta(self):
        self.assertEqual(
            self.redis.set('foo', 'bar', ex=timedelta(seconds=100)), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_set_px(self):
        self.assertEqual(self.redis.set('foo', 'bar', px=100), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_set_px_using_timedelta(self):
        self.assertEqual(
            self.redis.set('foo', 'bar', px=timedelta(milliseconds=100)), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_set_raises_wrong_ex(self):
        with self.assertRaises(ResponseError):
            self.redis.set('foo', 'bar', ex=-100)

    def test_set_using_timedelta_raises_wrong_ex(self):
        with self.assertRaises(ResponseError):
            self.redis.set('foo', 'bar', ex=timedelta(seconds=-100))

    def test_set_raises_wrong_px(self):
        with self.assertRaises(ResponseError):
            self.redis.set('foo', 'bar', px=-100)

    def test_set_using_timedelta_raises_wrong_px(self):
        with self.assertRaises(ResponseError):
            self.redis.set('foo', 'bar', px=timedelta(milliseconds=-100))

    def test_setex_raises_wrong_ex(self):
        with self.assertRaises(ResponseError):
            self.redis.setex('foo', -100, 'bar')

    def test_setex_using_timedelta_raises_wrong_ex(self):
        with self.assertRaises(ResponseError):
            self.redis.setex('foo', timedelta(seconds=-100), 'bar')

    def test_setnx(self):
        self.assertEqual(self.redis.setnx('foo', 'bar'), True)
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.setnx('foo', 'baz'), False)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_delete(self):
        self.redis['foo'] = 'bar'
        self.assertEqual(self.redis.delete('foo'), True)
        self.assertEqual(self.redis.get('foo'), None)

    def test_echo(self):
        self.assertEqual(self.redis.echo(b'hello'), b'hello')
        self.assertEqual(self.redis.echo('hello'), b'hello')

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

    # Tests for the list type.

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
            'foo', 0, -1),
            ['[12345L, \'banana\']', '[54321L, \'elephant\']'] if PY2 else
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

    def test_lpush_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lpush('foo', 'element')

    def test_llen(self):
        self.redis.lpush('foo', 'one')
        self.redis.lpush('foo', 'two')
        self.redis.lpush('foo', 'three')
        self.assertEqual(self.redis.llen('foo'), 3)

    def test_llen_no_exist(self):
        self.assertEqual(self.redis.llen('foo'), 0)

    def test_llen_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.llen('foo')

    def test_lrem_positive_count(self):
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

    def test_lrem_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lrem('foo', 0, 'element')

    def test_rpush(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('foo', 'three')
        self.redis.rpush('foo', 'four', 'five')
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'one', b'two', b'three', b'four', b'five'])

    def test_rpush_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.rpush('foo', 'element')

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

    def test_lpop_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lpop('foo')

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

    def test_lset_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lset('foo', 0, 'element')

    def test_rpushx(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpushx('foo', 'two')
        self.redis.rpushx('bar', 'three')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one', b'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_rpushx_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.rpushx('foo', 'element')

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

    def test_ltrim_expiry(self):
        self.redis.rpush('foo', 'one', 'two', 'three')
        self.redis.expire('foo', 10)
        self.redis.ltrim('foo', 1, 2)
        self.assertGreater(self.redis.ttl('foo'), 0)

    def test_ltrim_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.ltrim('foo', 1, -1)

    def test_lindex(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.lindex('foo', 0), b'one')
        self.assertEqual(self.redis.lindex('foo', 4), None)
        self.assertEqual(self.redis.lindex('bar', 4), None)

    def test_lindex_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lindex('foo', 0)

    def test_lpushx(self):
        self.redis.lpush('foo', 'two')
        self.redis.lpushx('foo', 'one')
        self.redis.lpushx('bar', 'one')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one', b'two'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [])

    def test_lpushx_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.lpushx('foo', 'element')

    def test_rpop(self):
        self.assertEqual(self.redis.rpop('foo'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.rpop('foo'), b'two')
        self.assertEqual(self.redis.rpop('foo'), b'one')
        self.assertEqual(self.redis.rpop('foo'), None)

    def test_rpop_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.rpop('foo')

    def test_linsert_before(self):
        self.redis.rpush('foo', 'hello')
        self.redis.rpush('foo', 'world')
        self.assertEqual(self.redis.linsert('foo', 'before', 'world', 'there'),
                         3)
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'hello', b'there', b'world'])

    def test_linsert_after(self):
        self.redis.rpush('foo', 'hello')
        self.redis.rpush('foo', 'world')
        self.assertEqual(self.redis.linsert('foo', 'after', 'hello', 'there'),
                         3)
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'hello', b'there', b'world'])

    def test_linsert_no_pivot(self):
        self.redis.rpush('foo', 'hello')
        self.redis.rpush('foo', 'world')
        self.assertEqual(self.redis.linsert('foo', 'after', 'goodbye', 'bar'),
                         -1)
        self.assertEqual(self.redis.lrange('foo', 0, -1),
                         [b'hello', b'world'])

    def test_linsert_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.linsert('foo', 'after', 'bar', 'element')

    def test_rpoplpush(self):
        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), None)
        self.assertEqual(self.redis.lpop('bar'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.redis.rpush('bar', 'one')

        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), b'two')
        self.assertEqual(self.redis.lrange('foo', 0, -1), [b'one'])
        self.assertEqual(self.redis.lrange('bar', 0, -1), [b'two', b'one'])

        # Catch instances where we store bytes and strings inconsistently
        # and thus bar = ['two', b'one']
        self.assertEqual(self.redis.lrem('bar', -1, 'two'), 1)

    def test_rpoplpush_to_nonexistent_destination(self):
        self.redis.rpush('foo', 'one')
        self.assertEqual(self.redis.rpoplpush('foo', 'bar'), b'one')
        self.assertEqual(self.redis.rpop('bar'), b'one')

    def test_rpoplpush_expiry(self):
        self.redis.rpush('foo', 'one')
        self.redis.rpush('bar', 'two')
        self.redis.expire('bar', 10)
        self.redis.rpoplpush('foo', 'bar')
        self.assertGreater(self.redis.ttl('bar'), 0)

    def test_rpoplpush_wrong_type(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('list', 'element')
        with self.assertRaises(redis.ResponseError):
            self.redis.rpoplpush('foo', 'list')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.lrange('list', 0, -1), [b'element'])
        with self.assertRaises(redis.ResponseError):
            self.redis.rpoplpush('list', 'foo')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.lrange('list', 0, -1), [b'element'])

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

    def test_blpop_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.blpop('foo', timeout=1)

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

    def test_brpop_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.brpop('foo', timeout=1)

    def test_brpoplpush_multi_keys(self):
        self.assertEqual(self.redis.lpop('bar'), None)
        self.redis.rpush('foo', 'one')
        self.redis.rpush('foo', 'two')
        self.assertEqual(self.redis.brpoplpush('foo', 'bar', timeout=1),
                         b'two')
        self.assertEqual(self.redis.lrange('bar', 0, -1), [b'two'])

        # Catch instances where we store bytes and strings inconsistently
        # and thus bar = ['two']
        self.assertEqual(self.redis.lrem('bar', -1, 'two'), 1)

    def test_brpoplpush_wrong_type(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('list', 'element')
        with self.assertRaises(redis.ResponseError):
            self.redis.brpoplpush('foo', 'list')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.lrange('list', 0, -1), [b'element'])
        with self.assertRaises(redis.ResponseError):
            self.redis.brpoplpush('list', 'foo')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.lrange('list', 0, -1), [b'element'])

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

    # Tests for the hash type.

    def test_hset_then_hget(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        self.assertEqual(self.redis.hget('foo', 'key'), b'value')

    def test_hset_update(self):
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 1)
        self.assertEqual(self.redis.hset('foo', 'key', 'value'), 0)

    def test_hset_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hset('foo', 'key', 'value')

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

    def test_hgetall_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hgetall('foo')

    def test_hexists(self):
        self.redis.hset('foo', 'bar', 'v1')
        self.assertEqual(self.redis.hexists('foo', 'bar'), 1)
        self.assertEqual(self.redis.hexists('foo', 'baz'), 0)
        self.assertEqual(self.redis.hexists('bar', 'bar'), 0)

    def test_hexists_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hexists('foo', 'key')

    def test_hkeys(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(set(self.redis.hkeys('foo')), set([b'k1', b'k2']))
        self.assertEqual(set(self.redis.hkeys('bar')), set([]))

    def test_hkeys_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hkeys('foo')

    def test_hlen(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(self.redis.hlen('foo'), 2)

    def test_hlen_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hlen('foo')

    def test_hvals(self):
        self.redis.hset('foo', 'k1', 'v1')
        self.redis.hset('foo', 'k2', 'v2')
        self.assertEqual(set(self.redis.hvals('foo')), set([b'v1', b'v2']))
        self.assertEqual(set(self.redis.hvals('bar')), set([]))

    def test_hvals_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hvals('foo')

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

    def test_hmget_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hmget('foo', 'key1', 'key2')

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

    def test_hdel_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hdel('foo', 'key')

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

    def test_hincrby_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hincrby('foo', 'key', 2)

    def test_hincrbyfloat(self):
        self.redis.hset('foo', 'counter', 0.0)
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 1.0)
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 2.0)
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 3.0)

    def test_hincrbyfloat_with_no_starting_value(self):
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 1.0)
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 2.0)
        self.assertEqual(self.redis.hincrbyfloat('foo', 'counter'), 3.0)

    def test_hincrbyfloat_with_range_param(self):
        self.assertAlmostEqual(
            self.redis.hincrbyfloat('foo', 'counter', 0.1), 0.1)
        self.assertAlmostEqual(
            self.redis.hincrbyfloat('foo', 'counter', 0.1), 0.2)
        self.assertAlmostEqual(
            self.redis.hincrbyfloat('foo', 'counter', 0.1), 0.3)

    def test_hincrbyfloat_on_non_float_value_raises_error(self):
        self.redis.hset('foo', 'counter', 'cat')
        with self.assertRaises(redis.ResponseError):
            self.redis.hincrbyfloat('foo', 'counter')

    def test_hincrbyfloat_with_non_float_amount_raises_error(self):
        with self.assertRaises(redis.ResponseError):
            self.redis.hincrbyfloat('foo', 'counter', 'cat')

    def test_hincrbyfloat_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hincrbyfloat('foo', 'key', 0.1)

    def test_hincrbyfloat_precision(self):
        x = 1.23456789123456789
        self.assertEqual(self.redis.hincrbyfloat('foo', 'bar', x), x)
        self.assertEqual(float(self.redis.hget('foo', 'bar')), x)

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
        self.assertEqual(
            self.redis.hgetall('foo'), {b'k1': b'True', b'k2': b'1'})

    def test_hmset_does_not_mutate_input_params(self):
        original = {'key': [123, 456]}
        self.redis.hmset('foo', original)
        self.assertEqual(original, {'key': [123, 456]})

    def test_hmset_wrong_type(self):
        self.redis.zadd('foo', 1, 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.hmset('foo', {'key': 'value'})

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

    def test_sadd_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.sadd('foo', 'member2')

    def test_scan_single(self):
        self.redis.set('foo1', 'bar1')
        self.assertEqual(self.redis.scan(match="foo*"), (0, [b'foo1']))

    def test_scan_iter_single_page(self):
        self.redis.set('foo1', 'bar1')
        self.redis.set('foo2', 'bar2')
        self.assertEqual(set(self.redis.scan_iter(match="foo*")),
                         set([b'foo1', b'foo2']))

    def test_scan_iter_multiple_pages(self):
        all_keys = key_val_dict(size=100)
        self.assertTrue(
            all(self.redis.set(k, v) for k, v in all_keys.items()))
        self.assertEqual(
            set(self.redis.scan_iter()),
            set(all_keys))

    def test_scan_iter_multiple_pages_with_match(self):
        all_keys = key_val_dict(size=100)
        self.assertTrue(
            all(self.redis.set(k, v) for k, v in all_keys.items()))
        # Now add a few keys that don't match the key:<number> pattern.
        self.redis.set('otherkey', 'foo')
        self.redis.set('andanother', 'bar')
        actual = set(self.redis.scan_iter(match='key:*'))
        self.assertEqual(actual, set(all_keys))

    def test_scan_multiple_pages_with_count_arg(self):
        all_keys = key_val_dict(size=100)
        self.assertTrue(
            all(self.redis.set(k, v) for k, v in all_keys.items()))
        self.assertEqual(
            set(self.redis.scan_iter(count=1000)),
            set(all_keys))

    def test_scan_all_in_single_call(self):
        all_keys = key_val_dict(size=100)
        self.assertTrue(
            all(self.redis.set(k, v) for k, v in all_keys.items()))
        # Specify way more than the 100 keys we've added.
        actual = self.redis.scan(count=1000)
        self.assertEqual(set(actual[1]), set(all_keys))
        self.assertEqual(actual[0], 0)

    def test_scard(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.scard('foo'), 2)

    def test_scard_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.scard('foo')

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

    def test_sdiff_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        self.redis.sadd('bar', 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.sdiff('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.sdiff('bar', 'foo')

    def test_sdiffstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sdiffstore('baz', 'foo', 'bar'), 1)

        # Catch instances where we store bytes and strings inconsistently
        # and thus baz = {'member1', b'member1'}
        self.redis.sadd('baz', 'member1')
        self.assertEqual(self.redis.scard('baz'), 1)

    def test_setrange(self):
        self.redis.set('foo', 'test')
        self.assertEqual(self.redis.setrange('foo', 1, 'aste'), 5)
        self.assertEqual(self.redis.get('foo'), b'taste')

        self.redis.set('foo', 'test')
        self.assertEqual(self.redis.setrange('foo', 1, 'a'), 4)
        self.assertEqual(self.redis.get('foo'), b'tast')

        self.assertEqual(self.redis.setrange('bar', 2, 'test'), 6)
        self.assertEqual(self.redis.get('bar'), b'\x00\x00test')

    def test_setrange_expiry(self):
        self.redis.set('foo', 'test', ex=10)
        self.redis.setrange('foo', 1, 'aste')
        self.assertGreater(self.redis.ttl('foo'), 0)

    def test_sinter(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sinter('foo', 'bar'), set([b'member2']))
        self.assertEqual(self.redis.sinter('foo'),
                         set([b'member1', b'member2']))

    def test_sinter_bytes_keys(self):
        foo = os.urandom(10)
        bar = os.urandom(10)
        self.redis.sadd(foo, 'member1')
        self.redis.sadd(foo, 'member2')
        self.redis.sadd(bar, 'member2')
        self.redis.sadd(bar, 'member3')
        self.assertEqual(self.redis.sinter(foo, bar), set([b'member2']))
        self.assertEqual(self.redis.sinter(foo), set([b'member1', b'member2']))

    def test_sinter_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        self.redis.sadd('bar', 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.sinter('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.sinter('bar', 'foo')

    def test_sinterstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sinterstore('baz', 'foo', 'bar'), 1)

        # Catch instances where we store bytes and strings inconsistently
        # and thus baz = {'member2', b'member2'}
        self.redis.sadd('baz', 'member2')
        self.assertEqual(self.redis.scard('baz'), 1)

    def test_sismember(self):
        self.assertEqual(self.redis.sismember('foo', 'member1'), False)
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.sismember('foo', 'member1'), True)

    def test_sismember_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.sismember('foo', 'member')

    def test_smembers(self):
        self.assertEqual(self.redis.smembers('foo'), set())

    def test_smembers_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.smembers('foo')

    def test_smove(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), True)
        self.assertEqual(self.redis.smembers('bar'), set([b'member1']))

    def test_smove_non_existent_key(self):
        self.assertEqual(self.redis.smove('foo', 'bar', 'member1'), False)

    def test_move_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        self.redis.sadd('bar', 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.smove('bar', 'foo', 'member')
        # Must raise the error before removing member from bar
        self.assertEqual(self.redis.smembers('bar'), set([b'member']))
        with self.assertRaises(redis.ResponseError):
            self.redis.smove('foo', 'bar', 'member')

    def test_spop(self):
        # This is tricky because it pops a random element.
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.spop('foo'), b'member1')
        self.assertEqual(self.redis.spop('foo'), None)

    def test_spop_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.spop('foo')

    def test_srandmember(self):
        self.redis.sadd('foo', 'member1')
        self.assertEqual(self.redis.srandmember('foo'), b'member1')
        # Shouldn't be removed from the set.
        self.assertEqual(self.redis.srandmember('foo'), b'member1')

    def test_srandmember_number(self):
        """srandmember works with the number argument."""
        self.assertEqual(self.redis.srandmember('foo', 2), [])
        self.redis.sadd('foo', b'member1')
        self.assertEqual(self.redis.srandmember('foo', 2), [b'member1'])
        self.redis.sadd('foo', b'member2')
        self.assertEqual(set(self.redis.srandmember('foo', 2)),
                         set([b'member1', b'member2']))
        self.redis.sadd('foo', b'member3')
        res = self.redis.srandmember('foo', 2)
        self.assertEqual(len(res), 2)

        if self.decode_responses:
            superset = set(['member1', 'member2', 'member3'])
        else:
            superset = set([b'member1', b'member2', b'member3'])

        for e in res:
            self.assertIn(e, superset)

    def test_srandmember_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.srandmember('foo')

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

    def test_srem_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.srem('foo', 'member')

    def test_sunion(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunion('foo', 'bar'),
                         set([b'member1', b'member2', b'member3']))

    def test_sunion_wrong_type(self):
        self.redis.zadd('foo', 1, 'member')
        self.redis.sadd('bar', 'member')
        with self.assertRaises(redis.ResponseError):
            self.redis.sunion('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.sunion('bar', 'foo')

    def test_sunionstore(self):
        self.redis.sadd('foo', 'member1')
        self.redis.sadd('foo', 'member2')
        self.redis.sadd('bar', 'member2')
        self.redis.sadd('bar', 'member3')
        self.assertEqual(self.redis.sunionstore('baz', 'foo', 'bar'), 3)
        self.assertEqual(self.redis.smembers('baz'),
                         set([b'member1', b'member2', b'member3']))

        # Catch instances where we store bytes and strings inconsistently
        # and thus baz = {b'member1', b'member2', b'member3', 'member3'}
        self.redis.sadd('baz', 'member3')
        self.assertEqual(self.redis.scard('baz'), 3)

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

    def test_zadd_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zadd('foo', 2, 'two')

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

    def test_zcard_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zcard('foo')

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

    def test_zcount_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zcount('foo', '-inf', '+inf')

    def test_zincrby(self):
        self.redis.zadd('foo', one=1)
        self.assertEqual(self.redis.zincrby('foo', 'one', 10), 11)
        self.assertEqual(self.redis.zrange('foo', 0, -1, withscores=True),
                         [(b'one', 11)])

    def test_zincrby_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zincrby('foo', 'one', 10)

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

    def test_zrange_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrange('foo', 0, -1)

    def test_zrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrank('foo', 'one'), 0)
        self.assertEqual(self.redis.zrank('foo', 'two'), 1)
        self.assertEqual(self.redis.zrank('foo', 'three'), 2)

    def test_zrank_non_existent_member(self):
        self.assertEqual(self.redis.zrank('foo', 'one'), None)

    def test_zrank_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrank('foo', 'one')

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
        self.assertEqual(self.redis.zrem('foo', 128), True)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'129'])

    def test_zrem_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrem('foo', 'bar')

    def test_zscore(self):
        self.redis.zadd('foo', one=54)
        self.assertEqual(self.redis.zscore('foo', 'one'), 54)

    def test_zscore_non_existent_member(self):
        self.assertIsNone(self.redis.zscore('foo', 'one'))

    def test_zscore_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zscore('foo', 'one')

    def test_zrevrank(self):
        self.redis.zadd('foo', one=1)
        self.redis.zadd('foo', two=2)
        self.redis.zadd('foo', three=3)
        self.assertEqual(self.redis.zrevrank('foo', 'one'), 2)
        self.assertEqual(self.redis.zrevrank('foo', 'two'), 1)
        self.assertEqual(self.redis.zrevrank('foo', 'three'), 0)

    def test_zrevrank_non_existent_member(self):
        self.assertEqual(self.redis.zrevrank('foo', 'one'), None)

    def test_zrevrank_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrank('foo', 'one')

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
        self.assertEqual(self.redis.zrevrange('foo', 0, 2),
                         [b'three', b'two_b', b'two'])
        self.assertEqual(self.redis.zrevrange('foo', 0, -1),
                         [b'three', b'two_b', b'two', b'one'])

    def test_zrevrange_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrange('foo', 0, 2)

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

    def test_zrangebyscore_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebyscore('foo', '(1', '(2')

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

    def test_zrevrangebyscore_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebyscore('foo', '(3', '(1')

    def test_zrangebylex(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zrangebylex('foo', b'(t', b'+'),
                         [b'three_a', b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'(t', b'[two_b'),
                         [b'three_a', b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'(t', b'(two_b'),
                         [b'three_a', b'two_a'])

        self.assertEqual(self.redis.zrangebylex('foo', b'[three_a', b'[two_b'),
                         [b'three_a', b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'(three_a', b'[two_b'),
                         [b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'-', b'(two_b'),
                         [b'one_a', b'three_a', b'two_a'])

        self.assertEqual(self.redis.zrangebylex('foo', b'[two_b', b'(two_b'),
                         [])
        # reversed max + and min - boundaries
        # these will be always empty, but allowed by redis
        self.assertEqual(self.redis.zrangebylex('foo', b'+', b'-'),
                         [])
        self.assertEqual(self.redis.zrangebylex('foo', b'+', b'[three_a'),
                         [])
        self.assertEqual(self.redis.zrangebylex('foo', b'[o', b'-'),
                         [])

    def test_zrangebylex_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b'-', b'+')

    def test_zlexcount(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zlexcount('foo', b'(t', b'+'),
                         3)

        self.assertEqual(self.redis.zlexcount('foo', b'(t', b'[two_b'),
                         3)

        self.assertEqual(self.redis.zlexcount('foo', b'(t', b'(two_b'),
                         2)

        self.assertEqual(self.redis.zlexcount('foo', b'[three_a', b'[two_b'),
                         3)
        self.assertEqual(self.redis.zlexcount('foo', b'(three_a', b'[two_b'),
                         2)

        self.assertEqual(self.redis.zlexcount('foo', b'-', b'(two_b'),
                         3)

        self.assertEqual(self.redis.zlexcount('foo', b'[two_b', b'(two_b'),
                         0)
        # reversed max + and min - boundaries
        # these will be always empty, but allowed by redis
        self.assertEqual(self.redis.zlexcount('foo', b'+', b'-'),
                         0)
        self.assertEqual(self.redis.zlexcount('foo', b'+', b'[three_a'),
                         0)
        self.assertEqual(self.redis.zlexcount('foo', b'[o', b'-'),
                         0)

    def test_zlexcount_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zlexcount('foo', b'-', b'+')

    def test_zrangebylex_with_limit(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zrangebylex('foo', b'-', b'+', 1, 2),
                         [b'three_a', b'two_a'])

        # negative offset no results
        self.assertEqual(self.redis.zrangebylex('foo', b'-', b'+', -1, 3),
                         [])

        # negative limit ignored
        self.assertEqual(self.redis.zrangebylex('foo', b'-', b'+', 0, -2),
                         [b'one_a', b'three_a', b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'-', b'+', 1, -2),
                         [b'three_a', b'two_a', b'two_b'])

        self.assertEqual(self.redis.zrangebylex('foo', b'+', b'-', 1, 1),
                         [])

    def test_zrangebylex_raises_error(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)

        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b'', b'[two_b')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b'-', b'two_b')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b'(t', b'two_b')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b't', b'+')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrangebylex('foo', b'[two_a', b'')

        with self.assertRaises(redis.RedisError):
            self.redis.zrangebylex('foo', b'(two_a', b'[two_b', 1)

    def test_zrevrangebylex(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zrevrangebylex('foo', b'+', b'(t'),
                         [b'two_b', b'two_a', b'three_a'])

        self.assertEqual(self.redis.zrevrangebylex('foo', b'[two_b', b'(t'),
                         [b'two_b', b'two_a', b'three_a'])

        self.assertEqual(self.redis.zrevrangebylex('foo', b'(two_b', b'(t'),
                         [b'two_a', b'three_a'])

        self.assertEqual(self.redis.zrevrangebylex('foo', b'[two_b', b'[three_a'),
                         [b'two_b', b'two_a', b'three_a'])

        self.assertEqual(self.redis.zrevrangebylex('foo', b'[two_b', b'(three_a'),
                         [b'two_b', b'two_a'])

        self.assertEqual(self.redis.zrevrangebylex('foo', b'(two_b', b'-'),
                         [b'two_a', b'three_a', b'one_a'])

        self.assertEqual(self.redis.zrangebylex('foo', b'(two_b', b'[two_b'),
                         [])
        # reversed max + and min - boundaries
        # these will be always empty, but allowed by redis
        self.assertEqual(self.redis.zrevrangebylex('foo', b'-', b'+'),
                         [])
        self.assertEqual(self.redis.zrevrangebylex('foo', b'[three_a', b'+'),
                         [])
        self.assertEqual(self.redis.zrevrangebylex('foo', b'-', b'[o'),
                         [])

    def test_zrevrangebylex_with_limit(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zrevrangebylex('foo', b'+', b'-', 1, 2),
                         [b'two_a', b'three_a'])

    def test_zrevrangebylex_raises_error(self):
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', three_a=0)

        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'[two_b', b'')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'two_b', b'-')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'two_b', b'(t')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'+', b't')

        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'', b'[two_a')

        with self.assertRaises(redis.RedisError):
            self.redis.zrevrangebylex('foo', b'[two_a', b'(two_b', 1)

    def test_zrevrangebylex_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zrevrangebylex('foo', b'+', b'-')

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

    def test_zremrangebyrank_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyrank('foo', 1, 3)

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

    def test_zremrangebyscore_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebyscore('foo', 0, 2)

    def test_zremrangebylex(self):
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', three_a=0)
        self.assertEqual(self.redis.zremrangebylex('foo', b'(three_a', b'[two_b'), 2)
        self.assertEqual(self.redis.zremrangebylex('foo', b'(three_a', b'[two_b'), 0)
        self.assertEqual(self.redis.zremrangebylex('foo', b'-', b'(o'), 0)
        self.assertEqual(self.redis.zremrangebylex('foo', b'-', b'[one_a'), 1)
        self.assertEqual(self.redis.zremrangebylex('foo', b'[tw', b'+'), 0)
        self.assertEqual(self.redis.zremrangebylex('foo', b'[t', b'+'), 1)
        self.assertEqual(self.redis.zremrangebylex('foo', b'[t', b'+'), 0)

    def test_zremrangebylex_error(self):
        self.redis.zadd('foo', two_a=0)
        self.redis.zadd('foo', two_b=0)
        self.redis.zadd('foo', one_a=0)
        self.redis.zadd('foo', three_a=0)
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebylex('foo', b'(t', b'two_b')

        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebylex('foo', b't', b'+')

        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebylex('foo', b'[two_a', b'')

    def test_zremrangebylex_badkey(self):
        self.assertEqual(self.redis.zremrangebylex('foo', b'(three_a', b'[two_b'), 0)

    def test_zremrangebylex_wrong_type(self):
        self.redis.sadd('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zremrangebylex('foo', b'bar', b'baz')

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

    def test_zunionstore_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zunionstore('baz', ['foo', 'bar'])

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

    def test_zinterstore_wrong_type(self):
        self.redis.set('foo', 'bar')
        with self.assertRaises(redis.ResponseError):
            self.redis.zinterstore('baz', ['foo', 'bar'])

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

        self.assertEqual(self.redis.sort("foo", start=0, num=1, desc=True),
                         [b"4"])

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

    def test_sort_wrong_type(self):
        self.redis.set('string', '3')
        with self.assertRaises(redis.ResponseError):
            self.redis.sort('string')

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
        self.assertEqual(res, [True, b'bar', 1, 2, [b'quux2', b'quux']])

        # Check side effects happened as expected.
        self.assertEqual(self.redis.lrange('baz', 0, -1), [b'quux2', b'quux'])

        # Check that the command buffer has been emptied.
        self.assertEqual(p.execute(), [])

    def test_pipeline_ignore_errors(self):
        """Test the pipeline ignoring errors when asked."""
        with self.redis.pipeline() as p:
            p.set('foo', 'bar')
            p.rename('baz', 'bats')
            with self.assertRaises(redis.exceptions.ResponseError):
                p.execute()
            self.assertEqual([], p.execute())
        with self.redis.pipeline() as p:
            p.set('foo', 'bar')
            p.rename('baz', 'bats')
            res = p.execute(raise_on_error=False)

            self.assertEqual([], p.execute())

            self.assertEqual(len(res), 2)
            self.assertIsInstance(res[1], redis.exceptions.ResponseError)

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

        self.assertEqual(res, [True, b'quux'])

    def test_pipeline_raises_when_watched_key_changed(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        self.addCleanup(p.reset)

        p.watch('greet', 'foo')
        nextf = fakenewsredis.to_bytes(p.get('foo')) + b'baz'
        # Simulate change happening on another thread.
        self.redis.rpush('greet', 'world')
        # Begin pipelining.
        p.multi()
        p.set('foo', nextf)

        with self.assertRaises(redis.WatchError):
            p.execute()

    def test_pipeline_succeeds_despite_unwatched_key_changed(self):
        # Same setup as before except for the params to the WATCH command.
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            # Only watch one of the 2 keys.
            p.watch('foo')
            nextf = fakenewsredis.to_bytes(p.get('foo')) + b'baz'
            # Simulate change happening on another thread.
            self.redis.rpush('greet', 'world')
            p.multi()
            p.set('foo', nextf)
            p.execute()

            # Check the commands were executed.
            self.assertEqual(self.redis.get('foo'), b'barbaz')
        finally:
            p.reset()

    def test_pipeline_succeeds_when_watching_nonexistent_key(self):
        self.redis.set('foo', 'bar')
        self.redis.rpush('greet', 'hello')
        p = self.redis.pipeline()
        try:
            # Also watch a nonexistent key.
            p.watch('foo', 'bam')
            nextf = fakenewsredis.to_bytes(p.get('foo')) + b'baz'
            # Simulate change happening on another thread.
            self.redis.rpush('greet', 'world')
            p.multi()
            p.set('foo', nextf)
            p.execute()

            # Check the commands were executed.
            self.assertEqual(self.redis.get('foo'), b'barbaz')
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

    def test_pipeline_transaction_value_from_callable(self):
        def callback(pipe):
            # No need to do anything here since we only want the return value
            return 'OUR-RETURN-VALUE'

        res = self.redis.transaction(callback, 'OUR-SEQUENCE-KEY',
                                     value_from_callable=True)

        self.assertEqual('OUR-RETURN-VALUE', res)

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

    def test_type(self):
        self.redis.set('string_key', "value")
        self.redis.lpush("list_key", "value")
        self.redis.sadd("set_key", "value")
        self.redis.zadd("zset_key", 1, "value")
        self.redis.hset('hset_key', 'key', 'value')

        self.assertEqual(self.redis.type('string_key'), b'string')
        self.assertEqual(self.redis.type('list_key'), b'list')
        self.assertEqual(self.redis.type('set_key'), b'set')
        self.assertEqual(self.redis.type('zset_key'), b'zset')
        self.assertEqual(self.redis.type('hset_key'), b'hash')
        self.assertEqual(self.redis.type('none_key'), b'none')

    @attr('slow')
    def test_pubsub_subscribe(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe("channel")
        sleep(1)
        expected_message = {'type': 'subscribe', 'pattern': None,
                            'channel': b'channel', 'data': 1}
        message = pubsub.get_message()
        keys = list(pubsub.channels.keys())

        key = keys[0]
        if not self.decode_responses:
            key = (key if type(key) == bytes
                   else bytes(key, encoding='utf-8'))

        self.assertEqual(len(keys), 1)
        self.assertEqual(key, b'channel')
        self.assertEqual(message, expected_message)

    @attr('slow')
    def test_pubsub_psubscribe(self):
        pubsub = self.redis.pubsub()
        pubsub.psubscribe("channel.*")
        sleep(1)
        expected_message = {'type': 'psubscribe', 'pattern': None,
                            'channel': b'channel.*', 'data': 1}

        message = pubsub.get_message()
        keys = list(pubsub.patterns.keys())
        self.assertEqual(len(keys), 1)
        self.assertEqual(message, expected_message)

    @attr('slow')
    def test_pubsub_unsubscribe(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe('channel-1', 'channel-2', 'channel-3')
        sleep(1)
        expected_message = {'type': 'unsubscribe', 'pattern': None,
                            'channel': b'channel-1', 'data': 2}
        pubsub.get_message()
        pubsub.get_message()
        pubsub.get_message()

        # unsubscribe from one
        pubsub.unsubscribe('channel-1')
        sleep(1)
        message = pubsub.get_message()
        keys = list(pubsub.channels.keys())
        self.assertEqual(message, expected_message)
        self.assertEqual(len(keys), 2)

        # unsubscribe from multiple
        pubsub.unsubscribe()
        sleep(1)
        pubsub.get_message()
        pubsub.get_message()
        keys = list(pubsub.channels.keys())
        self.assertEqual(message, expected_message)
        self.assertEqual(len(keys), 0)

    @attr('slow')
    def test_pubsub_punsubscribe(self):
        pubsub = self.redis.pubsub()
        pubsub.psubscribe('channel-1.*', 'channel-2.*', 'channel-3.*')
        sleep(1)
        expected_message = {'type': 'punsubscribe', 'pattern': None,
                            'channel': b'channel-1.*', 'data': 2}
        pubsub.get_message()
        pubsub.get_message()
        pubsub.get_message()

        # unsubscribe from one
        pubsub.punsubscribe('channel-1.*')
        sleep(1)
        message = pubsub.get_message()
        keys = list(pubsub.patterns.keys())
        self.assertEqual(message, expected_message)
        self.assertEqual(len(keys), 2)

        # unsubscribe from multiple
        pubsub.punsubscribe()
        sleep(1)
        pubsub.get_message()
        pubsub.get_message()
        keys = list(pubsub.patterns.keys())
        self.assertEqual(len(keys), 0)

    @attr('slow')
    def test_pubsub_listen(self):
        def _listen(pubsub, q):
            count = 0
            for message in pubsub.listen():
                q.put(message)
                count += 1
                if count == 4:
                    pubsub.close()

        channel = 'ch1'
        patterns = ['ch1*', 'ch[1]', 'ch?']
        pubsub = self.redis.pubsub()
        pubsub.subscribe(channel)
        pubsub.psubscribe(*patterns)
        sleep(1)
        msg1 = pubsub.get_message()
        msg2 = pubsub.get_message()
        msg3 = pubsub.get_message()
        msg4 = pubsub.get_message()
        self.assertEqual(msg1['type'], 'subscribe')
        self.assertEqual(msg2['type'], 'psubscribe')
        self.assertEqual(msg3['type'], 'psubscribe')
        self.assertEqual(msg4['type'], 'psubscribe')

        q = Queue()
        t = threading.Thread(target=_listen, args=(pubsub, q))
        t.start()
        msg = 'hello world'
        self.redis.publish(channel, msg)
        t.join()

        msg1 = q.get()
        msg2 = q.get()
        msg3 = q.get()
        msg4 = q.get()

        if self.decode_responses:
            bpatterns = patterns + [channel]
        else:
            bpatterns = [pattern.encode() for pattern in patterns]
            bpatterns.append(channel.encode())
        msg = msg.encode()
        self.assertEqual(msg1['data'], msg)
        self.assertIn(msg1['channel'], bpatterns)
        self.assertEqual(msg2['data'], msg)
        self.assertIn(msg2['channel'], bpatterns)
        self.assertEqual(msg3['data'], msg)
        self.assertIn(msg3['channel'], bpatterns)
        self.assertEqual(msg4['data'], msg)
        self.assertIn(msg4['channel'], bpatterns)

    @attr('slow')
    def test_pubsub_ignore_sub_messages_listen(self):
        def _listen(pubsub, q):
            count = 0
            for message in pubsub.listen():
                q.put(message)
                count += 1
                if count == 4:
                    pubsub.close()

        channel = 'ch1'
        patterns = ['ch1*', 'ch[1]', 'ch?']
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(channel)
        pubsub.psubscribe(*patterns)
        sleep(1)

        q = Queue()
        t = threading.Thread(target=_listen, args=(pubsub, q))
        t.start()
        msg = 'hello world'
        self.redis.publish(channel, msg)
        t.join()

        msg1 = q.get()
        msg2 = q.get()
        msg3 = q.get()
        msg4 = q.get()

        if self.decode_responses:
            bpatterns = patterns + [channel]
        else:
            bpatterns = [pattern.encode() for pattern in patterns]
            bpatterns.append(channel.encode())
        msg = msg.encode()
        self.assertEqual(msg1['data'], msg)
        self.assertIn(msg1['channel'], bpatterns)
        self.assertEqual(msg2['data'], msg)
        self.assertIn(msg2['channel'], bpatterns)
        self.assertEqual(msg3['data'], msg)
        self.assertIn(msg3['channel'], bpatterns)
        self.assertEqual(msg4['data'], msg)
        self.assertIn(msg4['channel'], bpatterns)

    @attr('slow')
    def test_pubsub_binary_message(self):
        if self.decode_responses:
            # Reading the non-UTF-8 message will break if decoding
            # responses.
            return

        def _listen(pubsub, q):
            for message in pubsub.listen():
                q.put(message)
                pubsub.close()

        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe('channel')
        sleep(1)

        q = Queue()
        t = threading.Thread(target=_listen, args=(pubsub, q))
        t.start()
        msg = b'\x00hello world\r\n\xff'
        self.redis.publish('channel', msg)
        t.join()

        received = q.get()
        self.assertEqual(received['data'], msg)

    def test_pfadd(self):
        key = "hll-pfadd"
        self.assertEqual(
            1, self.redis.pfadd(key, "a", "b", "c", "d", "e", "f", "g"))
        self.assertEqual(7, self.redis.pfcount(key))

    def test_pfcount(self):
        key1 = "hll-pfcount01"
        key2 = "hll-pfcount02"
        key3 = "hll-pfcount03"
        self.assertEqual(1, self.redis.pfadd(key1, "foo", "bar", "zap"))
        self.assertEqual(0, self.redis.pfadd(key1, "zap", "zap", "zap"))
        self.assertEqual(0, self.redis.pfadd(key1, "foo", "bar"))
        self.assertEqual(3, self.redis.pfcount(key1))
        self.assertEqual(1, self.redis.pfadd(key2, "1", "2", "3"))
        self.assertEqual(3, self.redis.pfcount(key2))
        self.assertEqual(6, self.redis.pfcount(key1, key2))
        self.assertEqual(1, self.redis.pfadd(key3, "foo", "bar", "zip"))
        self.assertEqual(3, self.redis.pfcount(key3))
        self.assertEqual(4, self.redis.pfcount(key1, key3))
        self.assertEqual(7, self.redis.pfcount(key1, key2, key3))

    def test_pfmerge(self):
        key1 = "hll-pfmerge01"
        key2 = "hll-pfmerge02"
        key3 = "hll-pfmerge03"
        self.assertEqual(1, self.redis.pfadd(key1, "foo", "bar", "zap", "a"))
        self.assertEqual(1, self.redis.pfadd(key2, "a", "b", "c", "foo"))
        self.assertTrue(self.redis.pfmerge(key3, key1, key2))
        self.assertEqual(6, self.redis.pfcount(key3))

    def test_scan(self):
        # Setup the data
        for ix in range(20):
            k = 'scan-test:%s' % ix
            v = 'result:%s' % ix
            self.redis.set(k, v)
        expected = self.redis.keys()
        self.assertEqual(20, len(expected))  # Ensure we know what we're testing

        # Test that we page through the results and get everything out
        results = []
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.scan(cursor, count=6)
            results.extend(data)
        self.assertSetEqual(set(expected), set(results))

        # Now test that the MATCH functionality works
        results = []
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.scan(cursor, match='*7', count=100)
            results.extend(data)
        self.assertIn(b'scan-test:7', results)
        self.assertIn(b'scan-test:17', results)
        self.assertEqual(2, len(results))

        # Test the match on iterator
        results = [r for r in self.redis.scan_iter(match='*7')]
        self.assertIn(b'scan-test:7', results)
        self.assertIn(b'scan-test:17', results)
        self.assertEqual(2, len(results))

    def test_sscan(self):
        # Setup the data
        name = 'sscan-test'
        for ix in range(20):
            k = 'sscan-test:%s' % ix
            self.redis.sadd(name, k)
        expected = self.redis.smembers(name)
        self.assertEqual(20, len(expected))  # Ensure we know what we're testing

        # Test that we page through the results and get everything out
        results = []
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.sscan(name, cursor, count=6)
            results.extend(data)
        self.assertSetEqual(set(expected), set(results))

        # Test the iterator version
        results = [r for r in self.redis.sscan_iter(name, count=6)]
        self.assertSetEqual(set(expected), set(results))

        # Now test that the MATCH functionality works
        results = []
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.sscan(name, cursor, match='*7', count=100)
            results.extend(data)
        self.assertIn(b'sscan-test:7', results)
        self.assertIn(b'sscan-test:17', results)
        self.assertEqual(2, len(results))

        # Test the match on iterator
        results = [r for r in self.redis.sscan_iter(name, match='*7')]
        self.assertIn(b'sscan-test:7', results)
        self.assertIn(b'sscan-test:17', results)
        self.assertEqual(2, len(results))

    def test_hscan(self):
        # Setup the data
        name = 'hscan-test'
        for ix in range(20):
            k = 'key:%s' % ix
            v = 'result:%s' % ix
            self.redis.hset(name, k, v)
        expected = self.redis.hgetall(name)
        self.assertEqual(20, len(expected))  # Ensure we know what we're testing

        # Test that we page through the results and get everything out
        results = {}
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.hscan(name, cursor, count=6)
            results.update(data)
        self.assertDictEqual(expected, results)

        # Test the iterator version
        results = {}
        for key, val in self.redis.hscan_iter(name, count=6):
            results[key] = val
        self.assertDictEqual(expected, results)

        # Now test that the MATCH functionality works
        results = {}
        cursor = '0'
        while cursor != 0:
            cursor, data = self.redis.hscan(name, cursor, match='*7', count=100)
            results.update(data)
        self.assertIn(b'key:7', results)
        self.assertIn(b'key:17', results)
        self.assertEqual(2, len(results))

        # Test the match on iterator
        results = {}
        for key, val in self.redis.hscan_iter(name, match='*7'):
            results[key] = val
        self.assertIn(b'key:7', results)
        self.assertIn(b'key:17', results)
        self.assertEqual(2, len(results))

    def test_ttl_should_return_minus_one_for_non_expiring_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.ttl('foo'), -1)

    def test_ttl_should_return_minus_two_for_non_existent_key(self):
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.ttl('foo'), -2)

    def test_pttl_should_return_minus_one_for_non_expiring_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.assertEqual(self.redis.pttl('foo'), -1)

    def test_pttl_should_return_minus_two_for_non_existent_key(self):
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.pttl('foo'), -2)

    def test_persist(self):
        self.redis.set('foo', 'bar', ex=20)
        self.redis.persist('foo')
        self.assertEqual(self.redis.ttl('foo'), -1)

    def test_set_existing_key_persists(self):
        self.redis.set('foo', 'bar', ex=20)
        self.redis.set('foo', 'foo')
        self.assertEqual(self.redis.ttl('foo'), -1)


class TestFakeRedis(unittest.TestCase):
    decode_responses = False

    def setUp(self):
        self.redis = self.create_redis()

    def tearDown(self):
        self.redis.flushall()
        del self.redis

    def assertInRange(self, value, start, end, msg=None):
        self.assertGreaterEqual(value, start, msg)
        self.assertLessEqual(value, end, msg)

    def create_redis(self, db=0):
        return fakenewsredis.FakeRedis(db=db)

    def test_setex(self):
        self.assertEqual(self.redis.setex('foo', 'bar', 100), True)
        self.assertEqual(self.redis.get('foo'), b'bar')

    def test_setex_using_timedelta(self):
        self.assertEqual(
            self.redis.setex('foo', 'bar', timedelta(seconds=100)), True)
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
        result = self.redis.zadd('foo', 'one', 1)
        self.assertEqual(result, 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'one'])

    def test_zadd_missing_required_params(self):
        with self.assertRaises(redis.RedisError):
            # Missing the 'score' param.
            self.redis.zadd('foo', 'one')
        with self.assertRaises(redis.RedisError):
            # Missing the 'value' param.
            self.redis.zadd('foo', None, score=1)
        with self.assertRaises(redis.RedisError):
            self.redis.zadd('foo')

    def test_zadd_with_single_keypair(self):
        result = self.redis.zadd('foo', bar=1)
        self.assertEqual(result, 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'bar'])

    def test_zadd_with_multiple_keypairs(self):
        result = self.redis.zadd('foo', bar=1, baz=9)
        self.assertEqual(result, 2)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'bar', b'baz'])

    def test_zadd_with_name_is_non_string(self):
        result = self.redis.zadd('foo', 1, 9)
        self.assertEqual(result, 1)
        self.assertEqual(self.redis.zrange('foo', 0, -1), [b'1'])

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
        with self.assertRaises(ResponseError):
            self.redis.psetex('foo', 0, 'bar')
        self.redis.psetex('foo', 500, 'bar')
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)

    @attr('slow')
    def test_psetex_expire_value_using_timedelta(self):
        with self.assertRaises(ResponseError):
            self.redis.psetex('foo', timedelta(seconds=0), 'bar')
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

    def test_expire_should_return_true_for_existing_key(self):
        self.redis.set('foo', 'bar')
        rv = self.redis.expire('foo', 1)
        self.assertIs(rv, True)

    def test_expire_should_return_false_for_missing_key(self):
        rv = self.redis.expire('missing', 1)
        self.assertIs(rv, False)

    @attr('slow')
    def test_expire_should_expire_key_using_timedelta(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expire('foo', timedelta(seconds=1))
        sleep(1.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    @attr('slow')
    def test_expire_should_expire_immediately_with_millisecond_timedelta(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.expire('foo', timedelta(milliseconds=750))
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    @attr('slow')
    def test_pexpire_should_expire_key(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.pexpire('foo', 150)
        sleep(0.2)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.pexpire('bar', 1), False)

    def test_pexpire_should_return_truthy_for_existing_key(self):
        self.redis.set('foo', 'bar')
        rv = self.redis.pexpire('foo', 1)
        self.assertIs(bool(rv), True)

    def test_pexpire_should_return_falsey_for_missing_key(self):
        rv = self.redis.pexpire('missing', 1)
        self.assertIs(bool(rv), False)

    @attr('slow')
    def test_pexpire_should_expire_key_using_timedelta(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.pexpire('foo', timedelta(milliseconds=750))
        sleep(0.5)
        self.assertEqual(self.redis.get('foo'), b'bar')
        sleep(0.5)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.pexpire('bar', 1), False)

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

    def test_expireat_should_return_true_for_existing_key(self):
        self.redis.set('foo', 'bar')
        rv = self.redis.expireat('foo', int(time() + 1))
        self.assertIs(rv, True)

    def test_expireat_should_return_false_for_missing_key(self):
        rv = self.redis.expireat('missing', int(time() + 1))
        self.assertIs(rv, False)

    @attr('slow')
    def test_pexpireat_should_expire_key_by_datetime(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.pexpireat('foo', datetime.now() + timedelta(milliseconds=150))
        sleep(0.2)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.pexpireat('bar', datetime.now()), False)

    @attr('slow')
    def test_pexpireat_should_expire_key_by_timestamp(self):
        self.redis.set('foo', 'bar')
        self.assertEqual(self.redis.get('foo'), b'bar')
        self.redis.pexpireat('foo', int(time() * 1000 + 150))
        sleep(0.2)
        self.assertEqual(self.redis.get('foo'), None)
        self.assertEqual(self.redis.expire('bar', 1), False)

    def test_pexpireat_should_return_true_for_existing_key(self):
        self.redis.set('foo', 'bar')
        rv = self.redis.pexpireat('foo', int(time() * 1000 + 150))
        self.assertIs(bool(rv), True)

    def test_pexpireat_should_return_false_for_missing_key(self):
        rv = self.redis.pexpireat('missing', int(time() * 1000 + 150))
        self.assertIs(bool(rv), False)

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
        # See https://github.com/antirez/redis/blob/unstable/src/db.c#L632
        ttl = 1000000000
        self.redis.expire('foo', ttl)
        self.assertEqual(self.redis.ttl('foo'), ttl)

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
        ttl = 1000000000
        # See https://github.com/antirez/redis/blob/unstable/src/db.c#L632
        self.redis.expire('foo', ttl)
        self.assertInRange(self.redis.pttl('foo'),
                           ttl * 1000 - d,
                           ttl * 1000)

    def test_ttls_should_always_be_long(self):
        self.redis.set('foo', 'bar')
        self.redis.expire('foo', 1)
        self.assertTrue(type(self.redis.ttl('foo')) is long)
        self.assertTrue(type(self.redis.pttl('foo')) is long)

    def test_expire_should_not_handle_floating_point_values(self):
        self.redis.set('foo', 'bar')
        with self.assertRaisesRegexp(
                redis.ResponseError, 'value is not an integer or out of range'):
            self.redis.expire('something_new', 1.2)
            self.redis.pexpire('something_new', 1000.2)
            self.redis.expire('some_unused_key', 1.2)
            self.redis.pexpire('some_unused_key', 1000.2)

    def test_lock(self):
        lock = self.redis.lock('foo')
        lock.acquire()
        self.assertTrue(self.redis.exists('foo'))
        lock.release()
        self.assertFalse(self.redis.exists('foo'))
        with self.redis.lock('bar'):
            self.assertTrue(self.redis.exists('bar'))
        self.assertFalse(self.redis.exists('bar'))


class DecodeMixin(object):
    decode_responses = True

    def assertEqual(self, a, b, msg=None):
        super(DecodeMixin, self).assertEqual(a, fakenewsredis._decode(b), msg)

    def assertIn(self, member, container, msg=None):
        super(DecodeMixin, self).assertIn(fakenewsredis._decode(member), container)

    def assertItemsEqual(self, a, b):
        super(DecodeMixin, self).assertItemsEqual(a, fakenewsredis._decode(b))


class TestFakeStrictRedisDecodeResponses(DecodeMixin, TestFakeStrictRedis):
    def create_redis(self, db=0):
        return fakenewsredis.FakeStrictRedis(db=db, decode_responses=True)


class TestFakeRedisDecodeResponses(DecodeMixin, TestFakeRedis):
    def create_redis(self, db=0):
        return fakenewsredis.FakeRedis(db=db, decode_responses=True)


@redis_must_be_running
class TestRealRedis(TestFakeRedis):
    def create_redis(self, db=0):
        return redis.Redis('localhost', port=6379, db=db)


@redis_must_be_running
class TestRealStrictRedis(TestFakeStrictRedis):
    def create_redis(self, db=0):
        return redis.StrictRedis('localhost', port=6379, db=db)


@redis_must_be_running
class TestRealRedisDecodeResponses(TestFakeRedisDecodeResponses):
    def create_redis(self, db=0):
        return redis.Redis('localhost', port=6379, db=db, decode_responses=True)


@redis_must_be_running
class TestRealStrictRedisDecodeResponses(TestFakeStrictRedisDecodeResponses):
    def create_redis(self, db=0):
        return redis.StrictRedis('localhost', port=6379, db=db, decode_responses=True)


class TestInitArgs(unittest.TestCase):
    def test_can_accept_any_kwargs(self):
        fakenewsredis.FakeRedis(foo='bar', bar='baz')
        fakenewsredis.FakeStrictRedis(foo='bar', bar='baz')

    def test_from_url(self):
        db = fakenewsredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/0')
        db.set('foo', 'bar')
        self.assertEqual(db.get('foo'), b'bar')

    def test_from_url_with_db_arg(self):
        db = fakenewsredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/0')
        db1 = fakenewsredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/1')
        db2 = fakenewsredis.FakeStrictRedis.from_url(
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
        db = fakenewsredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/a')
        self.assertEqual(db._db_num, 0)

    def test_can_pass_through_extra_args(self):
        db = fakenewsredis.FakeStrictRedis.from_url(
            'redis://username:password@localhost:6379/0',
            decode_responses=True)
        db.set('foo', 'bar')
        self.assertEqual(db.get('foo'), 'bar')


class TestImportation(unittest.TestCase):
    def test_searches_for_c_stdlib_and_raises_if_missing(self):
        """
        Verifies that fakenewsredis checks for both libc and msvcrt when
        looking for a strtod implementation and that it fails fast when neither
        is found.
        """

        import ctypes.util

        # Patch manually since unittest.mock.patch is not available in old Python versions
        old_find_library = ctypes.util.find_library

        searched_libraries = set()

        try:
            ctypes.util.find_library = lambda library: searched_libraries.add(library)

            with self.assertRaises(ImportError):
                reload(fakenewsredis)

            self.assertEqual(set(['c', 'msvcrt']), searched_libraries)
        finally:
            ctypes.util.find_library = old_find_library

            reload(fakenewsredis)


if __name__ == '__main__':
    unittest.main()
