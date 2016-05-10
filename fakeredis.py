import random
import warnings
import copy
from ctypes import CDLL, POINTER, c_double, c_char_p, pointer
from ctypes.util import find_library
import fnmatch
from collections import MutableMapping
from datetime import datetime, timedelta
import operator
import sys
import time
import re

import redis
from redis.exceptions import ResponseError
import redis.client

try:
    # Python 2.6, 2.7
    from Queue import Queue, Empty
except:
    # Python 3
    from queue import Queue, Empty

PY2 = sys.version_info[0] == 2

if not PY2:
    long = int


__version__ = '0.7.0'


if sys.version_info[0] == 2:
    text_type = unicode
    string_types = (str, unicode)
    redis_string_types = (str, unicode, bytes)
    byte_to_int = ord
    int_to_byte = chr

    def to_bytes(x, charset=sys.getdefaultencoding(), errors='strict'):
        if x is None:
            return None
        if isinstance(x, (bytes, bytearray, buffer)) or hasattr(x, '__str__'):
            return bytes(x)
        if isinstance(x, unicode):
            return x.encode(charset, errors)
        if hasattr(x, '__unicode__'):
            return unicode(x).encode(charset, errors)
        raise TypeError('expected bytes or unicode, not ' + type(x).__name__)

    def to_native(x, charset=sys.getdefaultencoding(), errors='strict'):
        if x is None or isinstance(x, str):
            return x
        return x.encode(charset, errors)

    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
    from urlparse import urlparse
else:
    text_type = str
    string_types = (str,)
    redis_string_types = (bytes, str)

    def byte_to_int(b):
        if isinstance(b, int):
            return b
        raise TypeError('an integer is required')

    int_to_byte = operator.methodcaller('to_bytes', 1, 'big')

    def to_bytes(x, charset=sys.getdefaultencoding(), errors='strict'):
        if x is None:
            return None
        if isinstance(x, (bytes, bytearray, memoryview)):
            return bytes(x)
        if isinstance(x, str):
            return x.encode(charset, errors)
        if hasattr(x, '__str__'):
            return str(x).encode(charset, errors)
        raise TypeError('expected bytes or str, not ' + type(x).__name__)

    def to_native(x, charset=sys.getdefaultencoding(), errors='strict'):
        if x is None or isinstance(x, str):
            return x
        return x.decode(charset, errors)

    iterkeys = lambda d: iter(d.keys())
    itervalues = lambda d: iter(d.values())
    iteritems = lambda d: iter(d.items())
    from urllib.parse import urlparse


DATABASES = {}

_libc_library = find_library('c') or find_library('msvcrt')

if not _libc_library:
    raise ImportError('fakeredis: unable to find libc or equivalent')

_libc = CDLL(_libc_library)
_libc.strtod.restype = c_double
_libc.strtod.argtypes = [c_char_p, POINTER(c_char_p)]
_strtod = _libc.strtod


def timedelta_total_seconds(delta):
    return delta.days * 86400 + delta.seconds + delta.microseconds / 1E6


class _StrKeyDict(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self._ex_keys = {}

    def __getitem__(self, key):
        self._update_expired_keys()
        return self._dict[to_bytes(key)]

    def __setitem__(self, key, value):
        self._dict[to_bytes(key)] = value

    def __delitem__(self, key):
        del self._dict[to_bytes(key)]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def expire(self, key, timestamp):
        self._ex_keys[key] = timestamp

    def expiring(self, key):
        if key not in self._ex_keys:
            return None
        return self._ex_keys[key]

    def _update_expired_keys(self):
        now = datetime.now()
        deleted = []
        for key in self._ex_keys:
            if now > self._ex_keys[key]:
                deleted.append(key)

        for key in deleted:
            del self._ex_keys[key]
            del self[key]

    def copy(self):
        new_copy = _StrKeyDict()
        for key, value in self._dict.items():
            new_copy[key] = value
        return new_copy

    def clear(self):
        super(_StrKeyDict, self).clear()
        self._ex_keys.clear()

    def to_bare_dict(self):
        return copy.deepcopy(self._dict)


class _ZSet(_StrKeyDict):
    redis_type = b'zset'


class _Hash(_StrKeyDict):
    redis_type = b'hash'


class FakeStrictRedis(object):
    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        url = urlparse(url)
        if db is None:
            try:
                db = int(url.path.replace('/', ''))
            except (AttributeError, ValueError):
                db = 0
        return cls(db=db)

    def __init__(self, db=0, charset='utf-8', errors='strict', **kwargs):
        if db not in DATABASES:
            DATABASES[db] = _StrKeyDict()
        self._db = DATABASES[db]
        self._db_num = db
        self._encoding = charset
        self._encoding_errors = errors
        self._pubsubs = []

    def flushdb(self):
        DATABASES[self._db_num].clear()
        return True

    def flushall(self):
        for db in DATABASES:
            DATABASES[db].clear()

        del self._pubsubs[:]

    # Basic key commands
    def append(self, key, value):
        self._db.setdefault(key, b'')
        self._db[key] += to_bytes(value)
        return len(self._db[key])

    def bitcount(self, name, start=0, end=-1):
        if end == -1:
            end = None
        else:
            end += 1
        try:
            s = self._db[name][start:end]
            return sum([bin(byte_to_int(l)).count('1') for l in s])
        except KeyError:
            return 0

    def decr(self, name, amount=1):
        try:
            self._db[name] = int(self._db.get(name, '0')) - amount
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        return self._db[name]

    def exists(self, name):
        return name in self._db
    __contains__ = exists

    def expire(self, name, time):
        return self._expire(name, time)

    def pexpire(self, name, millis):
        return self._expire(name, millis, 1000)

    def _expire(self, name, time, multiplier=1):
        if isinstance(time, timedelta):
            time = int(timedelta_total_seconds(time) * multiplier)
        if self.exists(name):
            self._db.expire(name, datetime.now() +
                            timedelta(seconds=time / multiplier))
            return True
        else:
            return False

    def expireat(self, name, when):
        if not isinstance(when, datetime):
            when = datetime.fromtimestamp(when)
        if self.exists(name):
            self._db.expire(name, when)
            return True
        else:
            return False

    def echo(self, value):
        if isinstance(value, text_type):
            return value.encode('utf-8')
        return value

    def get(self, name):
        value = self._db.get(name)
        if isinstance(value, _StrKeyDict):
            raise redis.ResponseError("WRONGTYPE Operation against a key "
                                      "holding the wrong kind of value")
        if value is not None:
            return to_bytes(value)

    def __getitem__(self, name):
        return self._db[name]

    def getbit(self, name, offset):
        """Returns a boolean indicating the value of ``offset`` in ``name``"""
        val = self._db.get(name, '\x00')
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        try:
            actual_val = byte_to_int(val[byte])
        except IndexError:
            return 0
        return 1 if (1 << actual_bitoffset) & actual_val else 0

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        val = self._db.get(name)
        self._db[name] = value
        return val

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        try:
            if not isinstance(amount, int):
                raise redis.ResponseError("value is not an integer or out "
                                          "of range.")
            self._db[name] = to_bytes(int(self._db.get(name, '0')) + amount)
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        return int(self._db[name])

    def incrby(self, name, amount=1):
        """
        Alias for command ``incr``
        """
        return self.incr(name, amount)

    def incrbyfloat(self, name, amount=1.0):
        try:
            self._db[name] = float(self._db.get(name, '0')) + amount
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not a valid float.")
        return self._db[name]

    def keys(self, pattern=None):
        return [key for key in self._db
                if not key or not pattern or
                fnmatch.fnmatch(to_native(key), to_native(pattern))]

    def mget(self, keys, *args):
        all_keys = self._list_or_args(keys, args)
        found = []
        if not all_keys:
            raise redis.ResponseError(
                "wrong number of arguments for 'mget' command")
        for key in all_keys:
            found.append(self._db.get(key))
        return found

    def mset(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise redis.RedisError(
                    'MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])
        for key, val in iteritems(kwargs):
            self.set(key, val)
        return True

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

    def move(self, name, db):
        pass

    def persist(self, name):
        pass

    def ping(self):
        return True

    def randomkey(self):
        pass

    def rename(self, src, dst):
        try:
            value = self._db[src]
        except KeyError:
            raise redis.ResponseError("No such key: %s" % src)
        self._db[dst] = value
        del self._db[src]
        return True

    def renamenx(self, src, dst):
        if dst in self._db:
            return False
        else:
            return self.rename(src, dst)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        if (not nx and not xx) or (nx and self._db.get(name, None) is None) \
                or (xx and not self._db.get(name, None) is None):
            if ex is not None:
                if isinstance(ex, timedelta):
                    ex = ex.seconds + ex.days * 24 * 3600
                if ex < 0:
                    raise ResponseError('invalid expire time in SETEX')
                if ex > 0:
                    self._db.expire(name, datetime.now() +
                                    timedelta(seconds=ex))
            elif px is not None:
                if isinstance(px, timedelta):
                    ms = int(px.microseconds / 1000)
                    px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
                if px < 0:
                    raise ResponseError('invalid expire time in SETEX')
                if px > 0:
                    self._db.expire(name, datetime.now() +
                                    timedelta(milliseconds=px))
            self._db[name] = to_bytes(value)
            return True
        else:
            return None

    __setitem__ = set

    def setbit(self, name, offset, value):
        val = self._db.get(name, b'\x00')
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        if len(val) - 1 < byte:
            # We need to expand val so that we can set the appropriate
            # bit.
            needed = byte - (len(val) - 1)
            val += b'\x00' * needed
        if value == 1:
            new_byte = byte_to_int(val[byte]) | (1 << actual_bitoffset)
        else:
            new_byte = byte_to_int(val[byte]) ^ (1 << actual_bitoffset)
        reconstructed = bytearray(val)
        reconstructed[byte] = new_byte
        self._db[name] = bytes(reconstructed)

    def setex(self, name, time, value):
        if isinstance(time, timedelta):
            time = int(timedelta_total_seconds(time))
        return self.set(name, value, ex=time)

    def psetex(self, name, time_ms, value):
        if isinstance(time_ms, timedelta):
            time_ms = int(timedelta_total_seconds(time_ms) * 1000)
        if time_ms == 0:
            raise ResponseError("invalid expire time in SETEX")
        return self.set(name, value, px=time_ms)

    def setnx(self, name, value):
        result = self.set(name, value, nx=True)
        # Real Redis returns False from setnx, but None from set(nx=...)
        if not result:
            return False
        return result

    def setrange(self, name, offset, value):
        pass

    def strlen(self, name):
        try:
            return len(self._db[name])
        except KeyError:
            return 0

    def substr(self, name, start, end=-1):
        if end == -1:
            end = None
        else:
            end += 1
        try:
            return self._db[name][start:end]
        except KeyError:
            return b''
    # Redis >= 2.0.0 this command is called getrange
    # according to the docs.
    getrange = substr

    def ttl(self, name):
        return self._ttl(name)

    def pttl(self, name):
        return self._ttl(name, 1000)

    def _ttl(self, name, multiplier=1):
        if name not in self._db:
            return None

        exp_time = self._db.expiring(name)
        if not exp_time:
            return None

        now = datetime.now()
        if now > exp_time:
            return None
        else:
            return round(((exp_time - now).days * 3600 * 24
                          + (exp_time - now).seconds
                          + (exp_time - now).microseconds / 1E6) * multiplier)

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

    def watch(self, *names):
        pass

    def unwatch(self):
        pass

    def delete(self, *names):
        deleted = 0
        for name in names:
            try:
                del self._db[name]
                if name in self._db._ex_keys:
                    del self._db._ex_keys[name]
                deleted += 1
            except KeyError:
                continue
        return deleted

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
            data = list(self._db[name])[:]
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
                return self._retrive_data_from_sort(data, get)
        except KeyError:
            return []

    def _retrive_data_from_sort(self, data, get):
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

    def lpush(self, name, *values):
        self._db.setdefault(name, [])[0:0] = list(reversed(
            [to_bytes(x) for x in values]))
        return len(self._db[name])

    def lrange(self, name, start, end):
        if end == -1:
            end = None
        else:
            end += 1
        return self._db.get(name, [])[start:end]

    def llen(self, name):
        return len(self._db.get(name, []))

    def lrem(self, name, count, value):
        value = to_bytes(value)
        a_list = self._db.get(name, [])
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

    def rpush(self, name, *values):
        self._db.setdefault(name, []).extend([to_bytes(x) for x in values])
        return len(self._db[name])

    def lpop(self, name):
        try:
            return self._db.get(name, []).pop(0)
        except IndexError:
            return None

    def lset(self, name, index, value):
        try:
            self._db.get(name, [])[index] = to_bytes(value)
        except IndexError:
            raise redis.ResponseError("index out of range")

    def rpushx(self, name, value):
        try:
            self._db[name].append(to_bytes(value))
        except KeyError:
            return

    def ltrim(self, name, start, end):
        try:
            val = self._db[name]
        except KeyError:
            return True
        if end == -1:
            end = None
        else:
            end += 1
        self._db[name] = val[start:end]
        return True

    def lindex(self, name, index):
        try:
            return self._db.get(name, [])[index]
        except IndexError:
            return None

    def lpushx(self, name, value):
        try:
            self._db[name].insert(0, to_bytes(value))
        except KeyError:
            return

    def rpop(self, name):
        try:
            return self._db.get(name, []).pop()
        except IndexError:
            return None

    def linsert(self, name, where, refvalue, value):
        index = self._db.get(name, []).index(to_bytes(refvalue))
        self._db.get(name, []).insert(index, to_bytes(value))

    def rpoplpush(self, src, dst):
        el = self.rpop(src)
        if el is not None:
            try:
                self._db[dst].insert(0, el)
            except KeyError:
                self._db[dst] = [el]
        return el

    def blpop(self, keys, timeout=0):
        # This has to be a best effort approximation which follows
        # these rules:
        # 1) For each of those keys see if there's something we can
        #    pop from.
        # 2) If this is not the case then simulate a timeout.
        # This means that there's not really any blocking behavior here.
        if isinstance(keys, string_types):
            keys = [to_bytes(keys)]
        else:
            keys = [to_bytes(k) for k in keys]
        for key in keys:
            if self._db.get(key, []):
                return (key, self._db[key].pop(0))

    def brpop(self, keys, timeout=0):
        if isinstance(keys, string_types):
            keys = [to_bytes(keys)]
        else:
            keys = [to_bytes(k) for k in keys]
        for key in keys:
            if self._db.get(key, []):
                return (key, self._db[key].pop())

    def brpoplpush(self, src, dst, timeout=0):
        el = self.rpop(src)
        if el is not None:
            try:
                self._db[dst].insert(0, el)
            except KeyError:
                self._db[dst] = [el]
        return el

    def hdel(self, name, *keys):
        h = self._db.get(name, {})
        rem = 0
        for k in keys:
            if k in h:
                del h[k]
                rem += 1
        return rem

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        if self._db.get(name, {}).get(key) is None:
            return 0
        else:
            return 1

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self._db.get(name, {}).get(key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        all_items = self._db.get(name, {})
        if hasattr(all_items, 'to_bare_dict'):
            all_items = all_items.to_bare_dict()
        return all_items

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        new = int(self._db.setdefault(name, _Hash()).get(key, '0')) + amount
        self._db[name][key] = new
        return new

    def hincrbyfloat(self, name, key, amount=1.0):
        """Increment the value of key in hash name by floating amount"""
        try:
            amount = float(amount)
        except ValueError:
            raise redis.ResponseError("value is not a valid float")
        try:
            current = float(self._db.setdefault(name, _Hash()).get(key, '0'))
        except ValueError:
            raise redis.ResponseError("hash value is not a valid float")
        new = current + amount
        self._db[name][key] = new
        return new

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return list(self._db.get(name, {}))

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return len(self._db.get(name, {}))

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        key_is_new = key not in self._db.get(name, {})
        self._db.setdefault(name, _Hash())[key] = to_bytes(value)
        return 1 if key_is_new else 0

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        if key in self._db.get(name, {}):
            return False
        self._db.setdefault(name, _Hash())[key] = to_bytes(value)
        return True

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
        self._db.setdefault(name, _Hash()).update(new_mapping)
        return True

    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        h = self._db.get(name, {})
        all_keys = self._list_or_args(keys, args)
        return [h.get(k) for k in all_keys]

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self._db.get(name, {}).values()

    def sadd(self, name, *values):
        "Add ``value`` to set ``name``"
        a_set = self._db.setdefault(name, set())
        card = len(a_set)
        a_set |= set(to_bytes(x) for x in values)
        return len(a_set) - card

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return len(self._db.get(name, set()))

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        diff = self._db.get(next(all_keys), set()).copy()
        for key in all_keys:
            diff -= self._db.get(key, set())
        return diff

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        diff = self.sdiff(keys, *args)
        self._db[dest] = diff
        return len(diff)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        intersect = self._db.get(next(all_keys), set()).copy()
        for key in all_keys:
            intersect.intersection_update(self._db.get(key, set()))
        return intersect

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        intersect = self.sinter(keys, *args)
        self._db[dest] = intersect
        return len(intersect)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return to_bytes(value) in self._db.get(name, set())

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self._db.get(name, set())

    def smove(self, src, dst, value):
        value = to_bytes(value)
        try:
            self._db.get(src, set()).remove(value)
            self._db.setdefault(dst, set()).add(value)
            return True
        except KeyError:
            return False

    def spop(self, name):
        "Remove and return a random member of set ``name``"
        try:
            return self._db.get(name, set()).pop()
        except KeyError:
            return None

    def srandmember(self, name):
        "Return a random member of set ``name``"
        members = self._db.get(name, set())
        if members:
            index = random.randint(0, len(members) - 1)
            return list(members)[index]

    def srem(self, name, *values):
        "Remove ``value`` from set ``name``"
        a_set = self._db.setdefault(name, set())
        card = len(a_set)
        a_set -= set(to_bytes(x) for x in values)
        return card - len(a_set)

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        all_keys = (to_bytes(x) for x in self._list_or_args(keys, args))
        union = self._db.get(next(all_keys), set()).copy()
        for key in all_keys:
            union.update(self._db.get(key, set()))
        return union

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        union = self.sunion(keys, *args)
        self._db[dest] = union
        return len(union)

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

    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        if len(args) % 2 != 0:
            raise redis.RedisError("ZADD requires an equal number of "
                                   "values and scores")
        zset = self._db.setdefault(name, _ZSet())
        added = 0
        for score, value in zip(*[args[i::2] for i in range(2)]):
            if value not in zset:
                added += 1
            try:
                zset[value] = float(score)
            except ValueError:
                raise redis.ResponseError("value is not a valid float")
        for value, score in kwargs.items():
            if value not in zset:
                added += 1
            try:
                zset[value] = float(score)
            except ValueError:
                raise redis.ResponseError("value is not a valid float")
        return added

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return len(self._db.get(name, {}))

    def zcount(self, name, min, max):
        found = 0
        filter_func = self._get_zelement_range_filter_func(min, max)
        for score in self._db.get(name, {}).values():
            if filter_func(score):
                found += 1
        return found

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        d = self._db.setdefault(name, _ZSet())
        score = d.get(value, 0) + amount
        d[value] = score
        return score

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
        valid_keys = set(self._db.get(list_keys[0], {}))
        for key in list_keys[1:]:
            valid_keys.intersection_update(self._db.get(key, {}))
        return self._zaggregate(dest, keys, aggregate,
                                lambda x: x in
                                valid_keys)

    def zrange(self, name, start, end, desc=False, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        """
        if end == -1:
            end = None
        else:
            end += 1
        all_items = self._db.get(name, {})
        if desc:
            reverse = True
        else:
            reverse = False
        in_order = self._get_zelements_in_order(all_items, reverse)
        items = in_order[start:end]
        if not withscores:
            return items
        else:
            return [(k, all_items[k]) for k in items]

    def _get_zelements_in_order(self, all_items, reverse=False):
        by_keyname = sorted(
            all_items.items(), key=lambda x: x[0], reverse=reverse)
        in_order = sorted(by_keyname, key=lambda x: x[1], reverse=reverse)
        return [el[0] for el in in_order]

    def zrangebyscore(self, name, min, max,
                      start=None, num=None, withscores=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        """
        return self._zrangebyscore(name, min, max, start, num, withscores,
                                   reverse=False)

    def _zrangebyscore(self, name, min, max, start, num, withscores, reverse):
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise redis.RedisError("``start`` and ``num`` must both "
                                   "be specified")
        all_items = self._db.get(name, {})
        in_order = self._get_zelements_in_order(all_items, reverse=reverse)
        filter_func = self._get_zelement_range_filter_func(min, max)
        matches = []
        for item in in_order:
            if filter_func(all_items[item]):
                matches.append(item)
        if start is not None:
            matches = matches[start:start + num]
        if withscores:
            return [(k, all_items[k]) for k in matches]
        return matches

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
        all_items = self._db.get(name, {})
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

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        all_items = self._db.get(name, {})
        in_order = sorted(all_items, key=lambda x: all_items[x])
        try:
            return in_order.index(to_bytes(value))
        except ValueError:
            return None

    def zrem(self, name, *values):
        "Remove member ``value`` from sorted set ``name``"
        z = self._db.get(name, {})
        rem = 0
        for v in values:
            if v in z:
                del z[v]
                rem += 1
        return rem

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        all_items = self._db.get(name, {})
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

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        all_items = self._db.get(name, {})
        filter_func = self._get_zelement_range_filter_func(min, max)
        removed = 0
        for key in all_items.copy():
            if filter_func(all_items[key]):
                del all_items[key]
                removed += 1
        return removed

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
        all_items = self._db.get(name, {})
        filter_func = self._get_zelement_lexrange_filter_func(min, max)
        removed = 0
        for key in all_items.copy():
            if filter_func(key):
                del all_items[key]
                removed += 1
        return removed

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
        all_items = self._db.get(name, {})
        filter_func = self._get_zelement_lexrange_filter_func(min, max)
        found = 0
        for key in all_items.copy():
            if filter_func(key):
                found += 1
        return found

    def zrevrange(self, name, start, num, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``num`` sorted in descending order.

        ``start`` and ``num`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs
        """
        return self.zrange(name, start, num, True, withscores)

    def zrevrangebyscore(self, name, max, min,
                         start=None, num=None, withscores=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        """
        return self._zrangebyscore(name, min, max, start, num, withscores,
                                   reverse=True)

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

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        num_items = len(self._db.get(name, {}))
        zrank = self.zrank(name, value)
        if zrank is not None:
            return num_items - self.zrank(name, value) - 1

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        try:
            return self._db[name][value]
        except KeyError:
            return None

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
            current_zset = self._db.get(key, {})
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
        # Based off of list_or_args from redis-py.
        # Returns a single list combining keys and args.
        # A string can be iterated, but indicates
        # keys wasn't passed as a list.
        if isinstance(keys, string_types):
            keys = [keys]
        if args:
            keys.extend(args)
        return keys

    def pipeline(self, transaction=True):
        """Return an object that can be used to issue Redis commands in a batch.

        Arguments --
            transaction (bool) -- whether the buffered commands
                are issued atomically. True by default.
        """
        return FakePipeline(self, transaction)

    def transaction(self, func, *keys):
        # We use a for loop instead of while
        # because if the test this is being used in
        # goes wrong we don't want an infinite loop!
        with self.pipeline() as p:
            for _ in range(5):
                try:
                    p.watch(*keys)
                    func(p)
                    return p.execute()
                except redis.WatchError:
                    continue
        raise redis.WatchError('Could not run transaction after 5 tries')

    def pubsub(self):
        """
        Returns a new FakePubSub instance
        """
        ps = FakePubSub()
        self._pubsubs.append(ps)

        return ps

    def publish(self, channel, message):
        """
        Loops throug all available pubsub objects and publishes the
        ``message`` to then for the given ``channel``.
        """
        count = 0
        for i, ps in enumerate(self._pubsubs):
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
        # subset =
        for val in data[cursor:result_cursor]:
            if not match or fnmatch.fnmatch(to_native(val), to_native(match)):
                result_data.append(val)
        if result_cursor >= len(data):
            result_cursor = 0
        return result_cursor, result_data

    def scan(self, cursor=0, match=None, count=None):
        return self._scan(self.keys(), int(cursor), match, count or 10)

    def sscan(self, name, cursor=0, match=None, count=None):
        return self._scan(self.smembers(name), int(cursor), match, count or 10)

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

    def execute(self, raise_on_error=True):
        """Run all the commands in the pipeline and return the results."""
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

    def __init__(self, *args, **kwargs):
        self.channels = {}
        self.patterns = {}
        self._q = Queue()
        self.subscribed = False

        self.ignore_subscribe_messages = kwargs['ignore_subscribe_messages']\
            if 'ignore_subscribe_messages' in kwargs else False

    def put(self, channel, message, message_type, pattern=None):
        """
        Utility function to be used as the publishing entrypoint for this
        pubsub object
        """
        if message_type in self.SUBSCRIBE_MESSAGE_TYPES or\
                message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            return self._send(message_type, None, channel, message)

        count = 0

        # Send the message on the given channel
        if channel in self.channels:
            count += self._send(message_type, None, channel, message)

        # See if any of the patterns match the given channel
        for pattern, pattern_obj in iteritems(self.patterns):
            match = re.match(pattern_obj['regex'], channel)
            if match:
                count += self._send('pmessage', pattern, channel, message)

        return count

    def _send(self, message_type, pattern, channel, data):
        msg = {
            'type': message_type,
            'pattern': pattern,
            'channel': channel.encode(),
            'data': data.encode() if type(data) == str else data
        }

        self._q.put(msg)

        return 1

    def psubscribe(self, *args, **kwargs):
        """
        Subcribe to channel patterns.
        """

        def _subscriber(pattern, handler):
            regex = self._parse_pattern(pattern)
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

    def _parse_pattern(self, pattern):
        temp_pattern = pattern
        if '?' in temp_pattern:
            temp_pattern = temp_pattern.replace('?', '.')

        if '*' in temp_pattern:
            temp_pattern = temp_pattern.replace('*', '.*')

        if ']' in temp_pattern:
            temp_pattern = temp_pattern.replace(']', ']?')

        return temp_pattern

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
            new_channels[channel] = handler

        subscribed_dict.update(new_channels)
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
                if channel in subscribed_dict:
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

            time.sleep(1)

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
        if the handler is avaialble. If the message is of type ``subscribe``
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
                channel = message['channel'].decode('utf-8')
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
