# -*- coding: utf-8 -*-
import random
import warnings
import copy
from ctypes import CDLL, POINTER, c_double, c_char_p, pointer
from ctypes.util import find_library
from collections import MutableMapping
from datetime import datetime, timedelta
import operator
import sys
import threading
import time
import types
import re
import functools
import uuid
from itertools import count, islice

import redis
from redis.exceptions import ResponseError, LockError, PubSubError
from redis.utils import dummy
import redis.client
from redis.client import PubSubWorkerThread

try:
    # Python 2.6, 2.7
    from Queue import Queue, Empty
except:
    # Python 3
    from queue import Queue, Empty

PY2 = sys.version_info[0] == 2


__version__ = '0.14.0'


if PY2:
    DEFAULT_ENCODING = 'utf-8'
    int_types = (int, long)  # noqa: F821
    text_type = unicode  # noqa: F821
    string_types = (str, unicode)  # noqa: F821
    redis_string_types = (str, unicode, bytes)  # noqa: F821
    byte_to_int = ord

    def to_bytes(x, charset=DEFAULT_ENCODING, errors='strict'):
        if isinstance(x, unicode):  # noqa: F821
            return x.encode(charset, errors)
        if isinstance(x, bytes):
            return x
        if isinstance(x, float):
            return repr(x)
        if isinstance(x, (bytearray, buffer)) or hasattr(x, '__str__'):  # noqa: F821
            return bytes(x)
        if hasattr(x, '__unicode__'):
            return unicode(x).encode(charset, errors)  # noqa: F821
        raise TypeError('expected bytes or unicode, not ' + type(x).__name__)

    def iteritems(d):
        return d.iteritems()

    from urlparse import urlparse
else:
    DEFAULT_ENCODING = sys.getdefaultencoding()
    long = int
    int_types = (int,)
    basestring = str
    text_type = str
    string_types = (str,)
    redis_string_types = (bytes, str)

    def byte_to_int(b):
        if isinstance(b, int):
            return b
        raise TypeError('an integer is required')

    def to_bytes(x, charset=sys.getdefaultencoding(), errors='strict'):
        if isinstance(x, (bytes, bytearray, memoryview)):  # noqa: F821
            return bytes(x)
        if isinstance(x, str):
            return x.encode(charset, errors)
        if isinstance(x, float):
            return repr(x).encode(charset, errors)
        if hasattr(x, '__str__'):
            return str(x).encode(charset, errors)
        raise TypeError('expected bytes or str, not ' + type(x).__name__)

    def iteritems(d):
        return iter(d.items())

    from urllib.parse import urlparse


DATABASES = {}

_libc_library = find_library('c') or find_library('msvcrt') or find_library('System')

if not _libc_library:
    raise ImportError('fakeredis: unable to find libc or equivalent')

_libc = CDLL(_libc_library)
_libc.strtod.restype = c_double
_libc.strtod.argtypes = [c_char_p, POINTER(c_char_p)]
_strtod = _libc.strtod


_WRONGTYPE_MSG = \
    "WRONGTYPE Operation against a key holding the wrong kind of value"


def timedelta_total_seconds(delta):
    return delta.days * 86400 + delta.seconds + delta.microseconds / 1E6


class _StrKeyDict(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)

    def __getitem__(self, key):
        return self._dict[to_bytes(key)]

    def __setitem__(self, key, value):
        self._dict[to_bytes(key)] = value

    def __delitem__(self, key):
        del self._dict[to_bytes(key)]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def copy(self):
        new_copy = self.__class__()
        new_copy.update(self._dict)
        return new_copy

    # Not strictly necessary, but MutableMapping implements it by popping one
    # item at a time, which may have odd effects in _ExpiringDict.
    def clear(self):
        self._dict.clear()


class _ExpiringDict(_StrKeyDict):
    def __getitem__(self, key):
        bytes_key = to_bytes(key)
        value, expiration = self._dict[bytes_key]
        if expiration is not None and datetime.now() > expiration:
            del self._dict[bytes_key]
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self._dict[to_bytes(key)] = (value, None)

    def expire(self, key, timestamp):
        bytes_key = to_bytes(key)
        value = self._dict[bytes_key][0]
        self._dict[bytes_key] = (value, timestamp)

    def setx(self, key, value, src=None):
        """Set a value, keeping the existing expiry time if any. If
        `src` is specified, it is used as the source of the expiry
        """
        if src is None:
            src = key
        try:
            _, expiration = self._dict[to_bytes(src)]
        except KeyError:
            expiration = None
        self._dict[to_bytes(key)] = (value, expiration)

    def persist(self, key):
        bytes_key = to_bytes(key)
        try:
            value, _ = self._dict[bytes_key]
        except KeyError:
            return
        self._dict[bytes_key] = (value, None)

    def expiring(self, key):
        return self._dict[to_bytes(key)][1]

    def __iter__(self):
        def generator():
            for key, (value, expiration) in iteritems(self._dict):
                if expiration is not None and datetime.now() > expiration:
                    continue
                yield key

        return generator()


class _ZSet(_StrKeyDict):
    redis_type = b'zset'


class _Hash(_StrKeyDict):
    redis_type = b'hash'


def DecodeGenerator(gen):
    for item in gen:
        yield _decode(item)


def _decode(value):
    if isinstance(value, text_type):
        return value
    elif isinstance(value, bytes):
        value = value.decode(DEFAULT_ENCODING)
    elif isinstance(value, dict):
        value = dict((_decode(k), _decode(v)) for k, v in value.items())
    elif isinstance(value, (list, set, tuple)):
        value = value.__class__(_decode(x) for x in value)
    elif isinstance(value, types.GeneratorType):
        value = DecodeGenerator(value)
    return value


def _make_decode_func(func):
    def decode_response(*args, **kwargs):
        val = _decode(func(*args, **kwargs))
        return val
    return decode_response


def _patch_responses(obj, decorator):
    for attr_name in dir(obj):
        attr = getattr(obj, attr_name)
        if not callable(attr) or attr_name.startswith('_'):
            continue
        func = decorator(attr)
        setattr(obj, attr_name, func)


def _lua_bool_ok(lua_runtime, value):
    # Inverse of bool_ok wrapper from redis-py
    return lua_runtime.table(ok='OK')


def _lua_reply(converter):
    def decorator(func):
        func._lua_reply = converter
        return func

    return decorator


def _remove_empty(func):
    @functools.wraps(func)
    def wrapper(self, key, *args, **kwargs):
        ret = func(self, key, *args, **kwargs)
        self._remove_if_empty(key)
        return ret

    return wrapper


def _compile_pattern(pattern):
    """Compile a glob pattern (e.g. for keys) to a bytes regex.

    fnmatch.fnmatchcase doesn't work for this, because it uses different
    escaping rules to redis, uses ! instead of ^ to negate a character set,
    and handles invalid cases (such as a [ without a ]) differently. This
    implementation was written by studying the redis implementation.
    """
    # It's easier to work with text than bytes, because indexing bytes
    # doesn't behave the same in Python 3. Latin-1 will round-trip safely.
    pattern = to_bytes(pattern).decode('latin-1')
    parts = ['^']
    i = 0
    L = len(pattern)
    while i < L:
        c = pattern[i]
        if c == '?':
            parts.append('.')
        elif c == '*':
            parts.append('.*')
        elif c == '\\':
            if i < L - 1:
                i += 1
            parts.append(re.escape(pattern[i]))
        elif c == '[':
            parts.append('[')
            i += 1
            if i < L and pattern[i] == '^':
                i += 1
                parts.append('^')
            while i < L:
                if pattern[i] == '\\':
                    i += 1
                    if i < L:
                        parts.append(re.escape(pattern[i]))
                elif pattern[i] == ']':
                    break
                elif i + 2 <= L and pattern[i + 1] == '-':
                    start = pattern[i]
                    end = pattern[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + '-' + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern[i]))
                i += 1
            parts.append(']')
        else:
            parts.append(re.escape(pattern[i]))
        i += 1
    parts.append('\\Z')
    regex = ''.join(parts).encode('latin-1')
    return re.compile(regex, re.S)


# This is a copy of redis.lock.Lock, but with some bugs fixed.
class _Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """
    def __init__(self, redis, name, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None, thread_local=True):
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.thread_local = bool(thread_local)
        self.local = threading.local() if self.thread_local else dummy()
        self.local.token = None
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    def __enter__(self):
        # force blocking, as otherwise the user would have to check whether
        # the lock was actually acquired or not.
        self.acquire(blocking=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=None, blocking_timeout=None):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.

        ``blocking_timeout`` specifies the maximum number of seconds to
        wait trying to acquire the lock.
        """
        sleep = self.sleep
        token = to_bytes(uuid.uuid1().hex)
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout
        while 1:
            if self.do_acquire(token):
                self.local.token = token
                return True
            if not blocking:
                return False
            if stop_trying_at is not None and time.time() > stop_trying_at:
                return False
            time.sleep(sleep)

    def do_acquire(self, token):
        if self.redis.setnx(self.name, token):
            if self.timeout:
                # convert to milliseconds
                timeout = int(self.timeout * 1000)
                self.redis.pexpire(self.name, timeout)
            return True
        return False

    def release(self):
        "Releases the already acquired lock"
        expected_token = self.local.token
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        self.local.token = None
        self.do_release(expected_token)

    def do_release(self, expected_token):
        name = self.name

        def execute_release(pipe):
            lock_value = to_bytes(pipe.get(name))
            if lock_value != expected_token:
                raise LockError("Cannot release a lock that's no longer owned")
            pipe.multi()
            pipe.delete(name)

        self.redis.transaction(execute_release, name)

    def extend(self, additional_time):
        """
        Adds more time to an already acquired lock.

        ``additional_time`` can be specified as an integer or a float, both
        representing the number of seconds to add.
        """
        if self.local.token is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_extend(additional_time)

    def do_extend(self, additional_time):
        pipe = self.redis.pipeline()
        pipe.watch(self.name)
        lock_value = to_bytes(pipe.get(self.name))
        if lock_value != self.local.token:
            raise LockError("Cannot extend a lock that's no longer owned")
        expiration = pipe.pttl(self.name)
        if expiration is None or expiration < 0:
            # Redis evicted the lock key between the previous get() and now
            # we'll handle this when we call pexpire()
            expiration = 0
        pipe.multi()
        pipe.pexpire(self.name, expiration + int(additional_time * 1000))

        try:
            response = pipe.execute()
        except redis.WatchError:
            # someone else acquired the lock
            raise LockError("Cannot extend a lock that's no longer owned")
        if not response[0]:
            # pexpire returns False if the key doesn't exist
            raise LockError("Cannot extend a lock that's no longer owned")
        return True


def _check_conn(func):
    """Used to mock connection errors"""
    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
        if not func.__self__.connected:
            raise redis.ConnectionError("FakeRedis is emulating a connection error.")
        return func(*args, **kwargs)
    return func_wrapper


def _locked(func):
    @functools.wraps(func)
    def func_wrapper(self, *args, **kwargs):
        with self._condition:
            ret = func(self, *args, **kwargs)
            # This is overkill as func might not even have modified the DB.
            # But fakeredis isn't intended to be high-performance.
            self._condition.notify_all()
            return ret
    return func_wrapper


class FakeStrictRedis(object):
    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        url = urlparse(url)
        if db is None:
            try:
                db = int(url.path.replace('/', ''))
            except (AttributeError, ValueError):
                db = 0
        return cls(db=db, **kwargs)

    def __init__(self, db=0, charset='utf-8', errors='strict',
                 decode_responses=False, singleton=True, connected=True, **kwargs):
        if singleton:
            self._dbs = DATABASES
        else:
            self._dbs = {}
        if db not in self._dbs:
            self._dbs[db] = _ExpiringDict()
        self._condition = threading.Condition()
        self._db = self._dbs[db]
        self._db_num = db
        self._encoding = charset
        self._encoding_errors = errors
        self._pubsubs = []
        self._decode_responses = decode_responses
        self.connected = connected
        _patch_responses(self, _check_conn)

        if decode_responses:
            _patch_responses(self, _make_decode_func)

    @_lua_reply(_lua_bool_ok)
    @_locked
    def flushdb(self):
        self._db.clear()
        return True

    @_lua_reply(_lua_bool_ok)
    @_locked
    def flushall(self):
        for db in self._dbs.values():
            db.clear()

        del self._pubsubs[:]
        return True

    def _remove_if_empty(self, key):
        try:
            value = self._db[key]
        except KeyError:
            pass
        else:
            if not value:
                del self._db[key]

    def _get_string(self, name, default=b''):
        value = self._db.get(name, default)
        # Allow None so that default can be set as None
        if not isinstance(value, bytes) and value is not None:
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _setdefault_string(self, name):
        value = self._db.setdefault(name, b'')
        if not isinstance(value, bytes):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    # Basic key commands
    @_locked
    def append(self, key, value):
        self._setdefault_string(key)
        self._db[key] += to_bytes(value)
        return len(self._db[key])

    @_locked
    def bitcount(self, name, start=0, end=-1):
        if end == -1:
            end = None
        else:
            end += 1
        try:
            s = self._get_string(name)[start:end]
            return sum([bin(byte_to_int(l)).count('1') for l in s])
        except KeyError:
            return 0

    @_locked
    def decr(self, name, amount=1):
        try:
            value = int(self._get_string(name, b'0')) - amount
            self._db.setx(name, to_bytes(value))
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        return value

    @_locked
    def exists(self, name):
        return name in self._db
    __contains__ = exists

    @_locked
    def expire(self, name, time):
        return self._expire(name, time)

    @_locked
    def pexpire(self, name, millis):
        return self._expire(name, millis, 1000)

    def _expire(self, name, time, multiplier=1):
        if isinstance(time, timedelta):
            time = int(timedelta_total_seconds(time) * multiplier)
        if not isinstance(time, int_types):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        if self.exists(name):
            self._db.expire(name, datetime.now() +
                            timedelta(seconds=time / float(multiplier)))
            return True
        else:
            return False

    @_locked
    def expireat(self, name, when):
        return self._expireat(name, when)

    @_locked
    def pexpireat(self, name, when):
        return self._expireat(name, when, 1000)

    def _expireat(self, name, when, multiplier=1):
        if not isinstance(when, datetime):
            when = datetime.fromtimestamp(when / float(multiplier))
        if self.exists(name):
            self._db.expire(name, when)
            return True
        else:
            return False

    @_locked
    def echo(self, value):
        if isinstance(value, text_type):
            return value.encode('utf-8')
        return value

    @_locked
    def get(self, name):
        value = self._get_string(name, None)
        if value is not None:
            return to_bytes(value)

    @_locked
    def __getitem__(self, name):
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    @_locked
    def getbit(self, name, offset):
        """Returns a boolean indicating the value of ``offset`` in ``name``"""
        val = self._get_string(name)
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        try:
            actual_val = byte_to_int(val[byte])
        except IndexError:
            return 0
        return 1 if (1 << actual_bitoffset) & actual_val else 0

    @_locked
    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        val = self._get_string(name, None)
        self._db[name] = to_bytes(value)
        return val

    @_locked
    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        try:
            if not isinstance(amount, int_types):
                raise redis.ResponseError("value is not an integer or out "
                                          "of range.")
            value = int(self._get_string(name, b'0')) + amount
            self._db.setx(name, to_bytes(value))
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        return value

    @_locked
    def incrby(self, name, amount=1):
        """
        Alias for command ``incr``
        """
        return self.incr(name, amount)

    @_locked
    def incrbyfloat(self, name, amount=1.0):
        try:
            value = float(self._get_string(name, b'0')) + amount
            self._db.setx(name, to_bytes(value))
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not a valid float.")
        return value

    @_locked
    def keys(self, pattern=None):
        if pattern is not None:
            regex = _compile_pattern(pattern)
        return [key for key in self._db if pattern is None or regex.match(key)]

    @_locked
    def mget(self, keys, *args):
        all_keys = self._list_or_args(keys, args)
        found = []
        if not all_keys:
            raise redis.ResponseError(
                "wrong number of arguments for 'mget' command")
        for key in all_keys:
            value = self._db.get(key)
            # Non-strings are returned as nil
            if not isinstance(value, bytes):
                value = None
            found.append(value)
        return found

    @_lua_reply(_lua_bool_ok)
    @_locked
    def mset(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError(
                    'MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])
        for key, val in iteritems(kwargs):
            self.set(key, val)
        return True

    @_locked
    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        if not any(k in self._db for k in mapping):
            for key, val in iteritems(mapping):
                self.set(key, val)
            return True
        return False

    @_locked
    def persist(self, name):
        self._db.persist(name)

    def ping(self):
        return True

    @_lua_reply(_lua_bool_ok)
    @_locked
    def rename(self, src, dst):
        try:
            value = self._db[src]
        except KeyError:
            raise redis.ResponseError("No such key: %s" % src)
        self._db.setx(dst, value, src=src)
        del self._db[src]
        return True

    @_locked
    def renamenx(self, src, dst):
        if dst in self._db:
            return False
        else:
            return self.rename(src, dst)

    @_locked
    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        if (not nx and not xx) or (nx and self._db.get(name, None) is None) \
                or (xx and not self._db.get(name, None) is None):
            if ex is not None:
                if isinstance(ex, timedelta):
                    ex = ex.seconds + ex.days * 24 * 3600
                if ex <= 0:
                    raise ResponseError('invalid expire time in SETEX')
                self._db[name] = to_bytes(value)
                self._db.expire(name, datetime.now() +
                                timedelta(seconds=ex))
            elif px is not None:
                if isinstance(px, timedelta):
                    ms = int(px.microseconds / 1000)
                    px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
                if px <= 0:
                    raise ResponseError('invalid expire time in SETEX')
                self._db[name] = to_bytes(value)
                self._db.expire(name, datetime.now() +
                                timedelta(milliseconds=px))
            else:
                self._db[name] = to_bytes(value)
            return True
        else:
            return None

    __setitem__ = set

    @_locked
    def setbit(self, name, offset, value):
        val = self._get_string(name, b'\x00')
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        if len(val) - 1 < byte:
            # We need to expand val so that we can set the appropriate
            # bit.
            needed = byte - (len(val) - 1)
            val += b'\x00' * needed
        old_byte = byte_to_int(val[byte])
        if value == 1:
            new_byte = old_byte | (1 << actual_bitoffset)
        else:
            new_byte = old_byte & ~(1 << actual_bitoffset)
        old_value = value if old_byte == new_byte else not value
        reconstructed = bytearray(val)
        reconstructed[byte] = new_byte
        self._db.setx(name, bytes(reconstructed))
        return bool(old_value)

    @_locked
    def setex(self, name, time, value):
        if isinstance(time, timedelta):
            time = int(timedelta_total_seconds(time))
        if not isinstance(time, int_types):
            raise ResponseError(
                'value is not an integer or out of range')
        return self.set(name, value, ex=time)

    @_locked
    def psetex(self, name, time_ms, value):
        if isinstance(time_ms, timedelta):
            time_ms = int(timedelta_total_seconds(time_ms) * 1000)
        if time_ms == 0:
            raise ResponseError("invalid expire time in SETEX")
        return self.set(name, value, px=time_ms)

    @_locked
    def setnx(self, name, value):
        result = self.set(name, value, nx=True)
        # Real Redis returns False from setnx, but None from set(nx=...)
        if not result:
            return False
        return result

    @_locked
    def setrange(self, name, offset, value):
        val = self._get_string(name, b"")
        if len(val) < offset:
            val += b'\x00' * (offset - len(val))
        val = val[0:offset] + to_bytes(value) + val[offset+len(value):]
        self._db.setx(name, val)
        return len(val)

    @_locked
    def strlen(self, name):
        return len(self._get_string(name))

    @_locked
    def substr(self, name, start, end=-1):
        if end == -1:
            end = None
        else:
            end += 1
        try:
            return self._get_string(name)[start:end]
        except KeyError:
            return b''
    # Redis >= 2.0.0 this command is called getrange
    # according to the docs.
    getrange = substr

    @_locked
    def ttl(self, name):
        return self._ttl(name)

    @_locked
    def pttl(self, name):
        return self._ttl(name, 1000)

    def _ttl(self, name, multiplier=1):
        if name not in self._db:
            return -2

        exp_time = self._db.expiring(name)
        if not exp_time:
            return -1

        now = datetime.now()
        if now > exp_time:
            return None
        else:
            return long(round(((exp_time - now).days * 3600 * 24 +
                        (exp_time - now).seconds +
                        (exp_time - now).microseconds / 1E6) * multiplier))

    @_locked
    def type(self, name):
        key = self._db.get(name)
        if hasattr(key.__class__, 'redis_type'):
            return key.redis_type
        if isinstance(key, redis_string_types):
            return b'string'
        elif isinstance(key, list):
            return b'list'
        elif isinstance(key, set):
            return b'set'
        else:
            assert key is None
            return b'none'

    @_lua_reply(_lua_bool_ok)
    def watch(self, *names):
        pass

    @_lua_reply(_lua_bool_ok)
    def unwatch(self):
        pass

    @_locked
    def delete(self, *names):
        deleted = 0
        for name in names:
            try:
                del self._db[name]
                deleted += 1
            except KeyError:
                continue
        return deleted

    @_locked
    def sort(self, name, start=None, num=None, by=None, get=None, desc=False,
             alpha=False, store=None):
        """Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        """
        if (start is None and num is not None) or \
                (start is not None and num is None):
            raise redis.RedisError(
                "RedisError: ``start`` and ``num`` must both be specified")
        try:
            data = self._db[name]
            if not isinstance(data, (list, set, _ZSet)):
                raise redis.ResponseError(_WRONGTYPE_MSG)
            data = list(data)
            if by is not None:
                # _sort_using_by_arg mutates data so we don't
                # need need a return value.
                self._sort_using_by_arg(data, by=by)
            elif not alpha:
                data.sort(key=self._strtod_key_func)
            else:
                data.sort()
            if desc:
                data = list(reversed(data))
            if not (start is None and num is None):
                data = data[start:start + num]
            if store is not None:
                self._db[store] = data
                return len(data)
            else:
                return self._retrieve_data_from_sort(data, get)
        except KeyError:
            return []

    @_locked
    def eval(self, script, numkeys, *keys_and_args):
        from lupa import LuaRuntime, LuaError

        if any(
            isinstance(numkeys, t) for t in (text_type, str, bytes)
        ):
            try:
                numkeys = int(numkeys)
            except ValueError:
                # Non-numeric string will be handled below.
                pass
        if not isinstance(numkeys, int_types):
            raise ResponseError("value is not an integer or out of range")
        elif numkeys > len(keys_and_args):
            raise ResponseError("Number of keys can't be greater than number of args")
        elif numkeys < 0:
            raise ResponseError("Number of keys can't be negative")

        keys_and_args = [to_bytes(v) for v in keys_and_args]
        lua_runtime = LuaRuntime(unpack_returned_tuples=True)

        set_globals = lua_runtime.eval(
            """
            function(keys, argv, redis_call, redis_pcall)
                redis = {}
                redis.call = redis_call
                redis.pcall = redis_pcall
                redis.error_reply = function(msg) return {err=msg} end
                redis.status_reply = function(msg) return {ok=msg} end
                KEYS = keys
                ARGV = argv
            end
            """
        )
        expected_globals = set()
        set_globals(
            lua_runtime.table_from(keys_and_args[:numkeys]),
            lua_runtime.table_from(keys_and_args[numkeys:]),
            functools.partial(self._lua_redis_call, lua_runtime, expected_globals),
            functools.partial(self._lua_redis_pcall, lua_runtime, expected_globals)
        )
        expected_globals.update(lua_runtime.globals().keys())

        try:
            result = lua_runtime.execute(script)
        except LuaError as ex:
            raise ResponseError(ex)

        self._check_for_lua_globals(lua_runtime, expected_globals)

        return self._convert_lua_result(result, nested=False)

    def _convert_redis_result(self, lua_runtime, result):
        if isinstance(result, dict):
            converted = [
                i
                for item in result.items()
                for i in item
            ]
            return lua_runtime.table_from(converted)
        elif isinstance(result, set):
            converted = sorted(
                self._convert_redis_result(lua_runtime, item)
                for item in result
            )
            return lua_runtime.table_from(converted)
        elif isinstance(result, (list, set, tuple)):
            converted = [
                self._convert_redis_result(lua_runtime, item)
                for item in result
            ]
            return lua_runtime.table_from(converted)
        elif isinstance(result, bool):
            return int(result)
        elif isinstance(result, float):
            return to_bytes(result)
        elif result is None:
            return False
        else:
            return result

    def _convert_lua_result(self, result, nested=True):
        from lupa import lua_type
        if lua_type(result) == 'table':
            for key in ('ok', 'err'):
                if key in result:
                    msg = self._convert_lua_result(result[key])
                    if not isinstance(msg, bytes):
                        raise ResponseError("wrong number or type of arguments")
                    if key == 'ok':
                        return msg
                    elif nested:
                        return ResponseError(msg)
                    else:
                        raise ResponseError(msg)
            # Convert Lua tables into lists, starting from index 1, mimicking the behavior of StrictRedis.
            result_list = []
            for index in count(1):
                if index not in result:
                    break
                item = result[index]
                result_list.append(self._convert_lua_result(item))
            return result_list
        elif isinstance(result, text_type):
            return to_bytes(result)
        elif isinstance(result, float):
            return int(result)
        elif isinstance(result, bool):
            return 1 if result else None
        return result

    def _check_for_lua_globals(self, lua_runtime, expected_globals):
        actual_globals = set(lua_runtime.globals().keys())
        if actual_globals != expected_globals:
            raise ResponseError(
                "Script attempted to set a global variables: %s" % ", ".join(
                    actual_globals - expected_globals
                )
            )

    def _lua_redis_pcall(self, lua_runtime, expected_globals, op, *args):
        try:
            return self._lua_redis_call(lua_runtime, expected_globals, op, *args)
        except Exception as ex:
            return lua_runtime.table_from({"err": str(ex)})

    def _lua_redis_call(self, lua_runtime, expected_globals, op, *args):
        # Check if we've set any global variables before making any change.
        self._check_for_lua_globals(lua_runtime, expected_globals)
        # These commands aren't necessarily all implemented, but if op is not one of these commands, we expect
        # a ResponseError for consistency with Redis
        commands = [
            'append', 'auth', 'bitcount', 'bitfield', 'bitop', 'bitpos', 'blpop', 'brpop', 'brpoplpush',
            'decr', 'decrby', 'del', 'dump', 'echo', 'eval', 'evalsha', 'exists', 'expire', 'expireat',
            'flushall', 'flushdb', 'geoadd', 'geodist', 'geohash', 'geopos', 'georadius', 'georadiusbymember',
            'get', 'getbit', 'getrange', 'getset', 'hdel', 'hexists', 'hget', 'hgetall', 'hincrby',
            'hincrbyfloat', 'hkeys', 'hlen', 'hmget', 'hmset', 'hscan', 'hset', 'hsetnx', 'hstrlen', 'hvals',
            'incr', 'incrby', 'incrbyfloat', 'info', 'keys', 'lindex', 'linsert', 'llen', 'lpop', 'lpush',
            'lpushx', 'lrange', 'lrem', 'lset', 'ltrim', 'mget', 'migrate', 'move', 'mset', 'msetnx',
            'object', 'persist', 'pexpire', 'pexpireat', 'pfadd', 'pfcount', 'pfmerge', 'ping', 'psetex',
            'psubscribe', 'pttl', 'publish', 'pubsub', 'punsubscribe', 'rename', 'renamenx', 'restore',
            'rpop', 'rpoplpush', 'rpush', 'rpushx', 'sadd', 'scan', 'scard', 'sdiff', 'sdiffstore', 'select',
            'set', 'setbit', 'setex', 'setnx', 'setrange', 'shutdown', 'sinter', 'sinterstore', 'sismember',
            'slaveof', 'slowlog', 'smembers', 'smove', 'sort', 'spop', 'srandmember', 'srem', 'sscan',
            'strlen', 'subscribe', 'sunion', 'sunionstore', 'swapdb', 'touch', 'ttl', 'type', 'unlink',
            'unsubscribe', 'wait', 'watch', 'zadd', 'zcard', 'zcount', 'zincrby', 'zinterstore', 'zlexcount',
            'zrange', 'zrangebylex', 'zrangebyscore', 'zrank', 'zrem', 'zremrangebylex', 'zremrangebyrank',
            'zremrangebyscore', 'zrevrange', 'zrevrangebylex', 'zrevrangebyscore', 'zrevrank', 'zscan',
            'zscore', 'zunionstore'
        ]

        op = op.lower()
        if op not in commands:
            raise ResponseError("Unknown Redis command called from Lua script")
        special_cases = {
            'del': FakeStrictRedis.delete,
            'decrby': FakeStrictRedis.decr,
            'incrby': FakeStrictRedis.incr
        }
        func = special_cases[op] if op in special_cases else getattr(FakeStrictRedis, op)
        result = func(self, *args)
        converter = getattr(func, '_lua_reply', self._convert_redis_result)
        return converter(lua_runtime, result)

    def _retrieve_data_from_sort(self, data, get):
        if get is not None:
            if isinstance(get, string_types):
                get = [get]
            new_data = []
            for k in data:
                for g in get:
                    single_item = self._get_single_item(k, g)
                    new_data.append(single_item)
            data = new_data
        return data

    def _get_single_item(self, k, g):
        g = to_bytes(g)
        if b'*' in g:
            g = g.replace(b'*', k)
            if b'->' in g:
                key, hash_key = g.split(b'->')
                single_item = self._db.get(key, {}).get(hash_key)
            else:
                single_item = self._db.get(g)
        elif b'#' in g:
            single_item = k
        else:
            single_item = None
        return single_item

    def _strtod_key_func(self, arg):
        # str()'ing the arg is important! Don't ever remove this.
        arg = to_bytes(arg)
        end = c_char_p()
        val = _strtod(arg, pointer(end))
        # real Redis also does an isnan check, not sure if
        # that's needed here or not.
        if end.value:
            raise redis.ResponseError(
                "One or more scores can't be converted into double")
        else:
            return val

    def _sort_using_by_arg(self, data, by):
        by = to_bytes(by)

        def _by_key(arg):
            key = by.replace(b'*', arg)
            if b'->' in by:
                key, hash_key = key.split(b'->')
                return self._db.get(key, {}).get(hash_key)
            else:
                return self._db.get(key)

        data.sort(key=_by_key)

    def _get_list(self, name):
        value = self._db.get(name, [])
        if not isinstance(value, list):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _get_list_or_none(self, name):
        """Like _get_list, but default value is None"""
        try:
            value = self._db[name]
            if not isinstance(value, list):
                raise redis.ResponseError(_WRONGTYPE_MSG)
            return value
        except KeyError:
            return None

    def _setdefault_list(self, name):
        value = self._db.setdefault(name, [])
        if not isinstance(value, list):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    @_locked
    def lpush(self, name, *values):
        self._setdefault_list(name)[0:0] = list(reversed(
            [to_bytes(x) for x in values]))
        return len(self._db[name])

    @_locked
    def lrange(self, name, start, end):
        if end == -1:
            end = None
        else:
            end += 1
        return self._get_list(name)[start:end]

    @_locked
    def llen(self, name):
        return len(self._get_list(name))

    @_locked
    @_remove_empty
    def lrem(self, name, count, value):
        value = to_bytes(value)
        a_list = self._get_list(name)
        found = []
        for i, el in enumerate(a_list):
            if el == value:
                found.append(i)
        if count > 0:
            indices_to_remove = found[:count]
        elif count < 0:
            indices_to_remove = found[count:]
        else:
            indices_to_remove = found
        # Iterating in reverse order to ensure the indices
        # remain valid during deletion.
        for index in reversed(indices_to_remove):
            del a_list[index]
        return len(indices_to_remove)

    @_locked
    def rpush(self, name, *values):
        self._setdefault_list(name).extend([to_bytes(x) for x in values])
        return len(self._db[name])

    @_locked
    @_remove_empty
    def lpop(self, name):
        try:
            return self._get_list(name).pop(0)
        except IndexError:
            return None

    @_lua_reply(_lua_bool_ok)
    @_locked
    def lset(self, name, index, value):
        try:
            lst = self._get_list_or_none(name)
            if lst is None:
                raise redis.ResponseError("no such key")
            lst[index] = to_bytes(value)
        except IndexError:
            raise redis.ResponseError("index out of range")
        return True

    @_locked
    def rpushx(self, name, value):
        self._get_list(name).append(to_bytes(value))

    @_lua_reply(_lua_bool_ok)
    @_locked
    def ltrim(self, name, start, end):
        val = self._get_list_or_none(name)
        if val is not None:
            if end == -1:
                end = None
            else:
                end += 1
            self._db.setx(name, val[start:end])
        return True

    @_locked
    def lindex(self, name, index):
        try:
            return self._get_list(name)[index]
        except IndexError:
            return None

    @_locked
    def lpushx(self, name, value):
        self._get_list(name).insert(0, to_bytes(value))

    @_locked
    @_remove_empty
    def rpop(self, name):
        try:
            return self._get_list(name).pop()
        except IndexError:
            return None

    @_locked
    def linsert(self, name, where, refvalue, value):
        if where.lower() not in ('before', 'after'):
            raise redis.ResponseError('syntax error')
        lst = self._get_list_or_none(name)
        if lst is None:
            return 0
        else:
            refvalue = to_bytes(refvalue)
            try:
                index = lst.index(refvalue)
            except ValueError:
                return -1
            if where.lower() == 'after':
                index += 1
            lst.insert(index, to_bytes(value))
            return len(lst)

    @_locked
    def rpoplpush(self, src, dst):
        # _get_list instead of _setdefault_list at this point because we
        # don't want to create the list if nothing gets popped.
        dst_list = self._get_list(dst)
        el = self.rpop(src)
        if el is not None:
            el = to_bytes(el)
            dst_list.insert(0, el)
            self._db.setx(dst, dst_list)
        return el

    def _blocking(self, timeout, func):
        if timeout is None:
            timeout = 0
        else:
            expire = datetime.now() + timedelta(seconds=timeout)
        while True:
            ret = func()
            if ret is not None:
                return ret
            if timeout == 0:
                self._condition.wait()
            else:
                wait_for = timedelta_total_seconds(expire - datetime.now())
                if wait_for <= 0:
                    break
                self._condition.wait(wait_for)
        # Timed out
        return None

    def _bpop(self, keys, timeout, pop):
        """Implements blpop and brpop"""
        if isinstance(keys, string_types):
            keys = [to_bytes(keys)]
        else:
            keys = [to_bytes(k) for k in keys]

        def try_pop():
            for key in keys:
                lst = self._get_list(key)
                if lst:
                    ret = (key, pop(lst))
                    self._remove_if_empty(key)
                    return ret
            return None

        return self._blocking(timeout, try_pop)

    @_locked
    def blpop(self, keys, timeout=0):
        return self._bpop(keys, timeout, lambda lst: lst.pop(0))

    @_locked
    def brpop(self, keys, timeout=0):
        return self._bpop(keys, timeout, lambda lst: lst.pop())

    @_locked
    def brpoplpush(self, src, dst, timeout=0):
        return self._blocking(timeout, lambda: self.rpoplpush(src, dst))

    def _get_hash(self, name):
        value = self._db.get(name, _Hash())
        if not isinstance(value, _Hash):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _setdefault_hash(self, name):
        value = self._db.setdefault(name, _Hash())
        if not isinstance(value, _Hash):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    @_locked
    @_remove_empty
    def hdel(self, name, *keys):
        h = self._get_hash(name)
        rem = 0
        for k in keys:
            if k in h:
                del h[k]
                rem += 1
        return rem

    @_locked
    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        if self._get_hash(name).get(key) is None:
            return 0
        else:
            return 1

    @_locked
    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self._get_hash(name).get(key)

    @_locked
    def hstrlen(self, name, key):
        "Returns the string length of the value associated with field in the hash stored at key"
        return len(self._get_hash(name).get(key, ""))

    @_locked
    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        all_items = dict()
        all_items.update(self._get_hash(name))
        return all_items

    @_locked
    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        new = int(self._setdefault_hash(name).get(key, b'0')) + amount
        self._db[name][key] = to_bytes(new)
        return new

    @_locked
    def hincrbyfloat(self, name, key, amount=1.0):
        """Increment the value of key in hash name by floating amount"""
        try:
            amount = float(amount)
        except ValueError:
            raise redis.ResponseError("value is not a valid float")
        try:
            current = float(self._setdefault_hash(name).get(key, b'0'))
        except ValueError:
            raise redis.ResponseError("hash value is not a valid float")
        new = current + amount
        self._db[name][key] = to_bytes(new)
        return new

    @_locked
    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return list(self._get_hash(name))

    @_locked
    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return len(self._get_hash(name))

    @_locked
    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        key_is_new = key not in self._get_hash(name)
        self._setdefault_hash(name)[key] = to_bytes(value)
        return 1 if key_is_new else 0

    @_locked
    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        if key in self._get_hash(name):
            return False
        self._setdefault_hash(name)[key] = to_bytes(value)
        return True

    @_locked
    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """
        if not mapping:
            raise redis.DataError("'hmset' with 'mapping' of length 0")
        new_mapping = {}
        for k, v in mapping.items():
            new_mapping[k] = to_bytes(v)
        self._setdefault_hash(name).update(new_mapping)
        return True

    @_locked
    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        h = self._get_hash(name)
        all_keys = self._list_or_args(keys, args)
        return [h.get(k) for k in all_keys]

    @_locked
    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return list(self._get_hash(name).values())

    def _get_set(self, name):
        value = self._db.get(name, set())
        if not isinstance(value, set):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _setdefault_set(self, name):
        value = self._db.setdefault(name, set())
        if not isinstance(value, set):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    @_locked
    def sadd(self, name, *values):
        "Add ``value`` to set ``name``"
        a_set = self._setdefault_set(name)
        card = len(a_set)
        a_set |= set(to_bytes(x) for x in values)
        return len(a_set) - card

    @_locked
    def scard(self, name):
        "Return the number of elements in set ``name``"
        return len(self._get_set(name))

    @_locked
    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        diff = self._get_set(next(all_keys)).copy()
        for key in all_keys:
            diff -= self._get_set(key)
        return diff

    @_locked
    @_remove_empty
    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        diff = self.sdiff(keys, *args)
        self._db[dest] = set(to_bytes(x) for x in diff)
        return len(diff)

    @_locked
    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        intersect = self._get_set(next(all_keys)).copy()
        for key in all_keys:
            intersect.intersection_update(self._get_set(key))
        return intersect

    @_locked
    @_remove_empty
    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        intersect = self.sinter(keys, *args)
        self._db[dest] = set(to_bytes(x) for x in intersect)
        return len(intersect)

    @_locked
    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return to_bytes(value) in self._get_set(name)

    @_locked
    def smembers(self, name):
        "Return all members of the set ``name``"
        return self._get_set(name).copy()

    @_locked
    @_remove_empty
    def smove(self, src, dst, value):
        value = to_bytes(value)
        src_set = self._get_set(src)
        dst_set = self._setdefault_set(dst)
        try:
            src_set.remove(value)
            dst_set.add(value)
            return True
        except KeyError:
            return False

    @_locked
    @_remove_empty
    def spop(self, name):
        "Remove and return a random member of set ``name``"
        try:
            return self._get_set(name).pop()
        except KeyError:
            return None

    @_locked
    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``.
        """
        members = self._get_set(name)
        if not members:
            if number is not None:
                return []
            else:
                return None

        if number is None:
            index = random.randint(0, len(members) - 1)
            return list(members)[index]
        elif len(members) <= number:
            # We return them all, shuffled.
            res = list(members)
            random.shuffle(res)
            return res
        else:
            member_list = list(members)
            return [
                member_list[i] for i
                in sorted(random.sample(range(len(members)), number))
            ]

    @_locked
    @_remove_empty
    def srem(self, name, *values):
        "Remove ``value`` from set ``name``"
        a_set = self._setdefault_set(name)
        card = len(a_set)
        a_set -= set(to_bytes(x) for x in values)
        return card - len(a_set)

    @_locked
    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        union = self._get_set(next(all_keys)).copy()
        for key in all_keys:
            union.update(self._get_set(key))
        return union

    @_locked
    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        union = self.sunion(keys, *args)
        self._db[dest] = set(to_bytes(x) for x in union)
        return len(union)

    def _get_zset(self, name):
        value = self._db.get(name, _ZSet())
        if not isinstance(value, _ZSet):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _get_anyset(self, name):
        value = self._db.get(name, set())
        if not isinstance(value, (_ZSet, set)):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _setdefault_zset(self, name):
        value = self._db.setdefault(name, _ZSet())
        if not isinstance(value, _ZSet):
            raise redis.ResponseError(_WRONGTYPE_MSG)
        return value

    def _get_zelement_range_filter_func(self, min_val, max_val):
        # This will return a filter function based on the
        # min and max values.  It takes a single argument
        # and return True if it matches the range filter
        # criteria, and False otherwise.

        # This will also handle the case when
        # min/max are '-inf', '+inf'.
        # It needs to handle exclusive intervals
        # where the min/max value is something like
        # '(0'
        #     a             <    x        <           b
        #     ^             ^             ^           ^
        # actual_min   left_comp     right_comp  actual_max
        left_comparator, actual_min = self._get_comparator_and_val(min_val)
        right_comparator, actual_max = self._get_comparator_and_val(max_val)

        def _matches(x):
            return (left_comparator(actual_min, x) and
                    right_comparator(x, actual_max))
        return _matches

    def _get_comparator_and_val(self, value):
        try:
            if isinstance(value, string_types) and value.startswith('('):
                comparator = operator.lt
                actual_value = float(value[1:])
            else:
                comparator = operator.le
                actual_value = float(value)
        except ValueError:
            raise redis.ResponseError('min or max is not a float')
        return comparator, actual_value

    def _get_zelement_lexrange_filter_func(self, min_str, max_str):
        # This will return a filter function based on the
        # min_str and max_str values.  It takes a single argument
        # and return True if it matches the range filter
        # criteria, and False otherwise.

        # This will handles inclusive '[' and exclusive '('
        # boundaries, as well as '-' and '+' which are
        # considered 'negative infinitiy string' and
        # maximum infinity string, which are handled by comparing
        # against empty string.
        #     a        < or <=    x     < or <=       b
        #     ^          ^                ^           ^
        # actual_min   left_comp     right_comp  actual_max
        min_str = to_bytes(min_str)
        max_str = to_bytes(max_str)

        left_comparator, actual_min = self._get_lexcomp_and_str(min_str)
        right_comparator, actual_max = self._get_lexcomp_and_str(max_str)

        def _matches(x):
            return (left_comparator(actual_min, x) and
                    right_comparator(x, actual_max))
        return _matches

    def _get_lexcomp_and_str(self, value):
        if value.startswith(b'('):
            comparator = operator.lt
            actual_value = value[1:]
        elif value.startswith(b'['):
            comparator = operator.le
            actual_value = value[1:]
        elif value == b'-':
            # negative infinity string -- all strings greater than
            # compares: '' < X
            comparator = operator.le
            actual_value = b''
        elif value == b'+':
            # positive infinity string -- all strings less than
            # compares: '' > X
            comparator = operator.ge
            actual_value = b''
        else:
            msg = ('min and max must start with ( or [, ' +
                   ' or min may be - and max may be +')
            raise redis.ResponseError(msg)

        return comparator, actual_value

    @_locked
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        if not args and not kwargs:
            raise redis.ResponseError("wrong number of arguments for 'zadd' command")
        if len(args) % 2 != 0:
            raise redis.RedisError("ZADD requires an equal number of "
                                   "values and scores")
        zset = self._setdefault_zset(name)
        old_len = len(zset)
        for score, value in zip(*[args[i::2] for i in range(2)]):
            try:
                zset[value] = float(score)
            except ValueError:
                raise redis.ResponseError("value is not a valid float")
        for value, score in kwargs.items():
            try:
                zset[value] = float(score)
            except ValueError:
                raise redis.ResponseError("value is not a valid float")
        return len(zset) - old_len

    @_locked
    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return len(self._get_zset(name))

    @_locked
    def zcount(self, name, min, max):
        found = 0
        filter_func = self._get_zelement_range_filter_func(min, max)
        for score in self._get_zset(name).values():
            if filter_func(score):
                found += 1
        return found

    @_locked
    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        d = self._setdefault_zset(name)
        score = d.get(value, 0) + amount
        d[value] = score
        return score

    @_locked
    @_remove_empty
    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        if not keys:
            raise redis.ResponseError("At least one key must be specified "
                                      "for ZINTERSTORE/ZUNIONSTORE")
        # keys can be a list or a dict so it needs to be converted to
        # a list first.
        list_keys = list(keys)
        valid_keys = set(self._get_anyset(list_keys[0]))
        for key in list_keys[1:]:
            valid_keys.intersection_update(self._get_anyset(key))
        return self._zaggregate(dest, keys, aggregate,
                                lambda x: x in
                                valid_keys)

    def _apply_score_cast_func(self, items, all_items, withscores, score_cast_func):
        if not withscores:
            return items
        elif score_cast_func is float:
            # Fast path for common case
            return [(k, all_items[k]) for k in items]
        elif self._decode_responses:
            return [(k, score_cast_func(_decode(to_bytes(all_items[k])))) for k in items]
        else:
            return [(k, score_cast_func(to_bytes(all_items[k]))) for k in items]

    @_locked
    def zrange(self, name, start, end, desc=False, withscores=False, score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if end == -1:
            end = None
        else:
            end += 1
        all_items = self._get_zset(name)
        if desc:
            reverse = True
        else:
            reverse = False
        in_order = self._get_zelements_in_order(all_items, reverse)
        items = in_order[start:end]
        return self._apply_score_cast_func(items, all_items, withscores, score_cast_func)

    def _get_zelements_in_order(self, all_items, reverse=False):
        by_keyname = sorted(
            all_items.items(), key=lambda x: x[0], reverse=reverse)
        in_order = sorted(by_keyname, key=lambda x: x[1], reverse=reverse)
        return [el[0] for el in in_order]

    @_locked
    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self._zrangebyscore(name, min, max, start, num, withscores, score_cast_func,
                                   reverse=False)

    def _zrangebyscore(self, name, min, max, start, num, withscores, score_cast_func, reverse):
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise redis.RedisError("``start`` and ``num`` must both "
                                   "be specified")
        all_items = self._get_zset(name)
        in_order = self._get_zelements_in_order(all_items, reverse=reverse)
        filter_func = self._get_zelement_range_filter_func(min, max)
        matches = []
        for item in in_order:
            if filter_func(all_items[item]):
                matches.append(item)
        if start is not None:
            matches = matches[start:start + num]
        return self._apply_score_cast_func(matches, all_items, withscores, score_cast_func)

    @_locked
    def zrangebylex(self, name, min, max,
                    start=None, num=None):
        """
        Returns lexicographically ordered values
        from sorted set ``name`` between values ``min`` and ``max``.

        The ``min`` and ``max`` params must:
            - start with ``(`` for exclusive boundary
            - start with ``[`` (inclusive boundary)
            - equal ``-`` for negative infinite string (start)
            - equal ``+`` for positive infinite string (stop)

        If ``start`` and ``num`` are specified, then a slice
        of the range is returned.

        """
        return self._zrangebylex(name, min, max, start, num,
                                 reverse=False)

    def _zrangebylex(self, name, min, max, start, num, reverse):
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise redis.RedisError("``start`` and ``num`` must both "
                                   "be specified")
        all_items = self._get_zset(name)
        in_order = self._get_zelements_in_order(all_items, reverse=reverse)
        filter_func = self._get_zelement_lexrange_filter_func(min, max)
        matches = []
        for item in in_order:
            if filter_func(item):
                matches.append(item)
        if start is not None:
            if num < 0:
                num = len(matches)
            matches = matches[start:start + num]
        return matches

    @_locked
    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        all_items = self._get_zset(name)
        in_order = sorted(all_items, key=lambda x: all_items[x])
        try:
            return in_order.index(to_bytes(value))
        except ValueError:
            return None

    @_locked
    @_remove_empty
    def zrem(self, name, *values):
        "Remove member ``value`` from sorted set ``name``"
        z = self._get_zset(name)
        rem = 0
        for v in values:
            if v in z:
                del z[v]
                rem += 1
        return rem

    @_locked
    @_remove_empty
    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        all_items = self._get_zset(name)
        in_order = self._get_zelements_in_order(all_items)
        num_deleted = 0
        if max == -1:
            max = None
        else:
            max += 1
        for key in in_order[min:max]:
            del all_items[key]
            num_deleted += 1
        return num_deleted

    @_locked
    @_remove_empty
    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        all_items = self._get_zset(name)
        filter_func = self._get_zelement_range_filter_func(min, max)
        removed = 0
        for key in all_items.copy():
            if filter_func(all_items[key]):
                del all_items[key]
                removed += 1
        return removed

    @_locked
    @_remove_empty
    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name``
        that are in lexicograpically between ``min`` and ``max``

        The ``min`` and ``max`` params must:
            - start with ``(`` for exclusive boundary
            - start with ``[`` (inclusive boundary)
            - equal ``-`` for negative infinite string (start)
            - equal ``+`` for positive infinite string (stop)
        """
        all_items = self._get_zset(name)
        filter_func = self._get_zelement_lexrange_filter_func(min, max)
        removed = 0
        for key in all_items.copy():
            if filter_func(key):
                del all_items[key]
                removed += 1
        return removed

    @_locked
    def zlexcount(self, name, min, max):
        """
        Returns a count of elements in the sorted set ``name``
        that are in lexicograpically between ``min`` and ``max``

        The ``min`` and ``max`` params must:
            - start with ``(`` for exclusive boundary
            - start with ``[`` (inclusive boundary)
            - equal ``-`` for negative infinite string (start)
            - equal ``+`` for positive infinite string (stop)
        """
        all_items = self._get_zset(name)
        filter_func = self._get_zelement_lexrange_filter_func(min, max)
        found = 0
        for key in all_items.copy():
            if filter_func(key):
                found += 1
        return found

    @_locked
    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self.zrange(name, start, end, True, withscores, score_cast_func)

    @_locked
    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self._zrangebyscore(name, min, max, start, num, withscores, score_cast_func,
                                   reverse=True)

    @_locked
    def zrevrangebylex(self, name, max, min,
                       start=None, num=None):
        """
        Returns reverse lexicographically ordered values
        from sorted set ``name`` between values ``min`` and ``max``.

        The ``min`` and ``max`` params must:
            - start with ``(`` for exclusive boundary
            - start with ``[`` (inclusive boundary)
            - equal ``-`` for negative infinite string (start)
            - equal ``+`` for positive infinite string (stop)

        If ``start`` and ``num`` are specified, then a slice
        of the range is returned.

        """
        return self._zrangebylex(name, min, max, start, num,
                                 reverse=True)

    @_locked
    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        num_items = len(self._get_zset(name))
        zrank = self.zrank(name, value)
        if zrank is not None:
            return num_items - self.zrank(name, value) - 1

    @_locked
    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        all_items = self._get_zset(name)
        try:
            return all_items[value]
        except KeyError:
            return None

    @_locked
    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        if not keys:
            raise redis.ResponseError("At least one key must be specified "
                                      "for ZINTERSTORE/ZUNIONSTORE")
        self._zaggregate(dest, keys, aggregate, lambda x: True)

    def _zaggregate(self, dest, keys, aggregate, should_include):
        new_zset = _ZSet()
        if aggregate is None:
            aggregate = 'SUM'
        # This is what the actual redis client uses, so we'll use
        # the same type check.
        if isinstance(keys, dict):
            keys_weights = [(k, keys[k]) for k in keys]
        else:
            keys_weights = [(k, 1) for k in keys]
        for key, weight in keys_weights:
            current_zset = self._get_anyset(key)
            if isinstance(current_zset, set):
                # When casting set to zset redis uses a default score of 1.0
                current_zset = dict((k, 1.0) for k in current_zset)
            for el in current_zset:
                if not should_include(el):
                    continue
                if el not in new_zset:
                    new_zset[el] = current_zset[el] * weight
                elif aggregate == 'SUM':
                    new_zset[el] += current_zset[el] * weight
                elif aggregate == 'MAX':
                    new_zset[el] = max([new_zset[el],
                                        current_zset[el] * weight])
                elif aggregate == 'MIN':
                    new_zset[el] = min([new_zset[el],
                                        current_zset[el] * weight])
        self._db[dest] = new_zset

    def _list_or_args(self, keys, args):
        # Returns a single list combining keys and args.
        # Copy of list_or_args from redis-py.
        try:
            iter(keys)
            # a string or bytes instance can be iterated, but indicates
            # keys wasn't passed as a list
            if isinstance(keys, (basestring, bytes)):
                keys = [keys]
        except TypeError:
            keys = [keys]
        if args:
            keys.extend(args)
        return keys

    def pipeline(self, transaction=True, shard_hint=None):
        """Return an object that can be used to issue Redis commands in a batch.

        Arguments --
            transaction (bool) -- whether the buffered commands
                are issued atomically. True by default.
        """
        return FakePipeline(self, transaction)

    def transaction(self, func, *keys, **kwargs):
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        watch_delay = kwargs.pop('watch_delay', None)
        # We use a for loop instead of while
        # because if the test this is being used in
        # goes wrong we don't want an infinite loop!
        with self.pipeline(True, shard_hint=shard_hint) as p:
            for _ in range(5):
                try:
                    if keys:
                        p.watch(*keys)
                    func_value = func(p)
                    exec_value = p.execute()
                    return func_value if value_from_callable else exec_value
                except redis.WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)

                    continue
        raise redis.WatchError('Could not run transaction after 5 tries')

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None,
             lock_class=None, thread_local=True):
        if lock_class is None:
            lock_class = _Lock
        return lock_class(self, name, timeout=timeout, sleep=sleep,
                          blocking_timeout=blocking_timeout,
                          thread_local=thread_local)

    @_locked
    def pubsub(self, ignore_subscribe_messages=False):
        """
        Returns a new FakePubSub instance
        """
        ps = FakePubSub(decode_responses=self._decode_responses,
                        ignore_subscribe_messages=ignore_subscribe_messages)
        self._pubsubs.append(ps)

        return ps

    @_locked
    def publish(self, channel, message):
        """
        Loops through all available pubsub objects and publishes the
        ``message`` to them for the given ``channel``.
        """
        count = 0
        for i, ps in list(enumerate(self._pubsubs)):
            if not ps.subscribed:
                del self._pubsubs[i]
                continue

            count += ps.put(channel, message, 'message')

        return count

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        "Adds the specified elements to the specified HyperLogLog."
        # Simulate the behavior of HyperLogLog by using SETs underneath to
        # approximate the behavior.
        result = self.sadd(name, *values)

        # Per the documentation:
        # - 1 if at least 1 HyperLogLog internal register was altered. 0 otherwise.
        return 1 if result > 0 else 0

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return len(self.sunion(*sources))

    @_lua_reply(_lua_bool_ok)
    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        self.sunionstore(dest, sources)
        return True

    # SCAN commands
    def _scan(self, keys, cursor, match, count):
        """
        This is the basis of most of the ``scan`` methods.

        This implementation is KNOWN to be un-performant, as it requires
        grabbing the full set of keys over which we are investigating subsets.
        """
        if cursor >= len(keys):
            return 0, []
        data = sorted(keys)
        result_cursor = cursor + count
        result_data = []

        if match is not None:
            regex = _compile_pattern(match)
            for val in islice(data, cursor, result_cursor):
                if regex.match(to_bytes(val)):
                    result_data.append(val)
        else:
            result_data = data[cursor:result_cursor]

        if result_cursor >= len(data):
            result_cursor = 0
        return result_cursor, result_data

    @_locked
    def scan(self, cursor=0, match=None, count=None):
        return self._scan(self.keys(), int(cursor), match, count or 10)

    @_locked
    def sscan(self, name, cursor=0, match=None, count=None):
        return self._scan(self.smembers(name), int(cursor), match, count or 10)

    @_locked
    def hscan(self, name, cursor=0, match=None, count=None):
        cursor, keys = self._scan(self.hkeys(name), int(cursor), match, count or 10)
        results = {}
        for k in keys:
            results[k] = self.hget(name, k)
        return cursor, results

    def scan_iter(self, match=None, count=None):
        # This is from redis-py
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def sscan_iter(self, name, match=None, count=None):
        # This is from redis-py
        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    def hscan_iter(self, name, match=None, count=None):
        # This is from redis-py
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item


class FakeRedis(FakeStrictRedis):
    def setex(self, name, value, time):
        return super(FakeRedis, self).setex(name, time, value)

    def lrem(self, name, value, num=0):
        return super(FakeRedis, self).lrem(name, num, value)

    def zadd(self, name, value=None, score=None, **pairs):
        """
        For each kwarg in ``pairs``, add that item and it's score to the
        sorted set ``name``.

        The ``value`` and ``score`` arguments are deprecated.
        """
        if value is not None or score is not None:
            if value is None or score is None:
                raise redis.RedisError(
                    "Both 'value' and 'score' must be specified to ZADD")
            warnings.warn(DeprecationWarning(
                "Passing 'value' and 'score' has been deprecated. "
                "Please pass via kwargs instead."))
            pairs = {str(value): score}
        elif not pairs:
            raise redis.RedisError("ZADD is missing kwargs param")
        return super(FakeRedis, self).zadd(name, **pairs)

    def ttl(self, name):
        r = super(FakeRedis, self).ttl(name)
        return r if r >= 0 else None

    def pttl(self, name):
        r = super(FakeRedis, self).pttl(name)
        return r if r >= 0 else None


class FakePipeline(object):
    """Helper class for FakeStrictRedis to implement pipelines.

    A pipeline is a collection of commands that
    are buffered until you call ``execute``, at which
    point they are called sequentially and a list
    of their return values is returned.

    """
    def __init__(self, owner, transaction=True):
        """Create a pipeline for the specified FakeStrictRedis instance.

        Arguments --
            owner -- a FakeStrictRedis instance.

        """
        self.owner = owner
        self.transaction = transaction
        self.commands = []
        self.need_reset = False
        self.is_immediate = False
        self.watching = {}

    def __getattr__(self, name):
        """Magic method to allow FakeStrictRedis commands to be called.

        Returns a method that records the command for later.

        """
        if not hasattr(self.owner, name):
            raise AttributeError('%r: does not have attribute %r' %
                                 (self.owner, name))

        def meth(*args, **kwargs):
            if self.is_immediate:
                # Special mode during watch_multi sequence.
                return getattr(self.owner, name)(*args, **kwargs)
            self.commands.append((name, args, kwargs))
            return self

        setattr(self, name, meth)
        return meth

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __len__(self):
        return len(self.commands)

    def execute(self, raise_on_error=True):
        """Run all the commands in the pipeline and return the results."""
        if not self.commands:
            return []
        try:
            if self.watching:
                mismatches = [
                    (k, v, u) for (k, v, u) in
                    [(k, v, self.owner._db.get(k))
                        for (k, v) in self.watching.items()]
                    if v != u]
                if mismatches:
                    self.commands = []
                    self.watching = {}
                    raise redis.WatchError(
                        'Watched key%s %s changed' % (
                            '' if len(mismatches) == 1 else
                            's', ', '.join(k for (k, _, _) in mismatches)))
            if raise_on_error:
                ret = [getattr(self.owner, name)(*args, **kwargs)
                       for name, args, kwargs in self.commands]
            else:
                ret = []
                for name, args, kwargs in self.commands:
                    try:
                        ret.append(getattr(self.owner, name)(*args, **kwargs))
                    except Exception as exc:
                        ret.append(exc)
            return ret
        finally:
            # Redis-py will reset in all cases, so do that.
            self.commands = []
            self.watching = {}

    def watch(self, *keys):
        self.watching.update((key, copy.deepcopy(self.owner._db.get(key)))
                             for key in keys)
        self.need_reset = True
        self.is_immediate = True

    def multi(self):
        self.is_immediate = False

    def reset(self):
        self.need_reset = False


class FakePubSub(object):

    PUBLISH_MESSAGE_TYPES = ['message', 'pmessage']
    SUBSCRIBE_MESSAGE_TYPES = ['subscribe', 'psubscribe']
    UNSUBSCRIBE_MESSAGE_TYPES = ['unsubscribe', 'punsubscribe']
    PATTERN_MESSAGE_TYPES = ['psubscribe', 'punsubscribe']
    LISTEN_DELAY = 0.1          # delay between listen loops (seconds)

    def __init__(self, decode_responses=False, connected=True, *args, **kwargs):
        self.channels = {}
        self.patterns = {}
        self._q = Queue()
        self.subscribed = False
        self.connected = connected
        _patch_responses(self, _check_conn)
        if decode_responses:
            _patch_responses(self, _make_decode_func)
        self._decode_responses = decode_responses
        self.ignore_subscribe_messages = kwargs.get(
            'ignore_subscribe_messages', False)

    def _normalize(self, value):
        value = to_bytes(value)
        return _decode(value) if self._decode_responses else value

    def _normalize_keys(self, data):
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        return dict([(self._normalize(k), v) for k, v in iteritems(data)])

    def put(self, channel, message, message_type):
        """
        Utility function to be used as the publishing entrypoint for this
        pubsub object
        """
        channel = self._normalize(channel)
        if message_type in self.SUBSCRIBE_MESSAGE_TYPES or\
                message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            return self._send(message_type, None, channel, message)

        count = 0
        message = self._normalize(message)

        # Send the message on the given channel
        if channel in self.channels:
            count += self._send(message_type, None, channel, message)

        # See if any of the patterns match the given channel
        for pattern, pattern_obj in iteritems(self.patterns):
            match = pattern_obj['regex'].match(to_bytes(channel))
            if match:
                count += self._send('pmessage', pattern, channel, message)

        return count

    def _send(self, message_type, pattern, channel, data):
        msg = {
            'type': message_type,
            'pattern': pattern,
            'channel': channel,
            'data': data
        }

        self._q.put(msg)

        return 1

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns.
        """

        def _subscriber(pattern, handler):
            regex = _compile_pattern(pattern)
            return {
                'regex': regex,
                'handler': handler
            }

        total_subscriptions =\
            len(self.channels.keys()) + len(self.patterns.keys())
        self._subscribe(self.patterns, 'psubscribe', total_subscriptions,
                        _subscriber, *args, **kwargs)

    def punsubscribe(self, *args):
        """
        Unsubscribes from one or more patterns.
        """
        total_subscriptions =\
            len(self.channels.keys()) + len(self.patterns.keys())
        self._usubscribe(self.patterns, 'punsubscribe', total_subscriptions,
                         *args)

    def subscribe(self, *args, **kwargs):
        """
        Subscribes to one or more given ``channels``.
        """

        def _subscriber(channel, handler):
            return handler

        total_subscriptions =\
            len(self.channels.keys()) + len(self.patterns.keys())
        self._subscribe(self.channels, 'subscribe', total_subscriptions,
                        _subscriber, *args, **kwargs)

    def _subscribe(self, subscribed_dict, message_type, total_subscriptions,
                   subscriber, *args, **kwargs):

        new_channels = {}
        if args:
            for arg in args:
                new_channels[arg] = subscriber(arg, None)

        for channel, handler in iteritems(kwargs):
            new_channels[channel] = subscriber(channel, handler)

        subscribed_dict.update(self._normalize_keys(new_channels))
        self.subscribed = True

        for channel in new_channels:
            total_subscriptions += 1
            self.put(channel, long(total_subscriptions), message_type)

    def unsubscribe(self, *args):
        """
        Unsubscribes from one or more given ``channels``.
        """
        total_subscriptions =\
            len(self.channels.keys()) + len(self.patterns.keys())
        self._usubscribe(self.channels, 'unsubscribe', total_subscriptions,
                         *args)

    def _usubscribe(self, subscribed_dict, message_type, total_subscriptions,
                    *args):

        if args:
            for channel in args:
                if self._normalize(channel) in subscribed_dict:
                    total_subscriptions -= 1
                    self.put(channel, long(total_subscriptions), message_type)
        else:
            for channel in subscribed_dict:
                total_subscriptions -= 1
                self.put(channel, long(total_subscriptions), message_type)
            subscribed_dict.clear()

        if total_subscriptions == 0:
            self.subscribed = False

    def listen(self):
        """
        Listens for queued messages and yields the to the calling process
        """
        while self.subscribed:
            message = self.get_message()
            if message:
                yield message
                continue

            time.sleep(self.LISTEN_DELAY)

    def close(self):
        """
        Stops the listen function by calling unsubscribe
        """
        self.unsubscribe()
        self.punsubscribe()

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """
        Returns the next available message.
        """

        try:
            message = self._q.get(True, timeout)
            return self.handle_message(message, ignore_subscribe_messages)
        except Empty:
            return None

    def handle_message(self, message, ignore_subscribe_messages=False):
        """
        Parses a pubsub message. It invokes the handler of a message type,
        if the handler is available. If the message is of type ``subscribe``
        and ignore_subscribe_messages if True, then it returns None. Otherwise,
        it returns the message.
        """
        message_type = message['type']
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            subscribed_dict = None
            if message_type == 'punsubscribe':
                subscribed_dict = self.patterns
            else:
                subscribed_dict = self.channels

            try:
                channel = message['channel']
                del subscribed_dict[channel]
            except:
                pass

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            handler = None
            if message_type == 'pmessage':
                pattern = self.patterns.get(message['pattern'], None)
                if pattern:
                    handler = pattern['handler']
            else:
                handler = self.channels.get(message['channel'], None)
            if handler:
                handler(message)
                return None
        else:
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    def run_in_thread(self, sleep_time=0, daemon=False):
        for channel, handler in iteritems(self.channels):
            if handler is None:
                raise PubSubError("Channel: '%s' has no handler registered" % (channel,))
        for pattern, handler in iteritems(self.patterns):
            if handler is None:
                raise PubSubError("Pattern: '%s' has no handler registered" % (channel,))

        thread = PubSubWorkerThread(self, sleep_time, daemon=daemon)
        thread.start()
        return thread
