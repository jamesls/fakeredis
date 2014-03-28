import random
import warnings
import copy
from ctypes import CDLL, POINTER, c_double, c_char_p, pointer
from ctypes.util import find_library
import fnmatch
from urlparse import urlparse
from collections import MutableMapping
from datetime import datetime, timedelta
import redis
from redis.exceptions import ResponseError
import redis.client


__version__ = '0.4.2'
DATABASES = {}

_libc = CDLL(find_library('c'))
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
        return self._dict[str(key)]

    def __setitem__(self, key, value):
        self._dict[str(key)] = value

    def __delitem__(self, key):
        del self._dict[str(key)]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def expire(self, key, timestamp):
        self._ex_keys[key] = timestamp

    def expiring(self, key):
        if not key in self._ex_keys.keys():
            return None
        return self._ex_keys[key]

    def _update_expired_keys(self):
        now = datetime.now()
        deleted = []
        for key in self._ex_keys.keys():
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

    def __init__(self, db=0, **kwargs):
        if db not in DATABASES:
            DATABASES[db] = _StrKeyDict()
        self._db = DATABASES[db]
        self._db_num = db


    def flushdb(self):
        DATABASES[self._db_num].clear()
        return True

    def flushall(self):
        for db in DATABASES:
            DATABASES[db].clear()

    # Basic key commands
    def append(self, key, value):
        self._db[key] += value
        return len(self._db[key])

    def bitcount(self, name, start=0, end=-1):
        if end == -1:
            end = None
        else:
            end += 1
        try:
            s = self._db[name][start:end]
            return sum([bin(ord(l)).count('1') for l in s])
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
        if isinstance(time, timedelta):
            time = int(timedelta_total_seconds(time))
        if self.exists(name):
            self._db.expire(name, datetime.now() + timedelta(seconds=time))
        else:
            return False

    def expireat(self, name, when):
        if not self.exists(name):
            return False
        if isinstance(when, datetime):
            self._db.expire(name, when)
        else:
            self._db.expire(name, datetime.fromtimestamp(when))

    def get(self, name):
        value = self._db.get(name)
        if value is not None:
            return str(value)

    def __getitem__(self, name):
        return self._db[name]

    def getbit(self, name, offset):
        """Returns a boolean indicating the value of ``offset`` in ``name``"""
        val = self._db.get(name, '\x00')
        byte = offset / 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        try:
            actual_val = ord(val[byte])
        except IndexError:
            return 0
        return 1 if (1 << actual_bitoffset) & actual_val else 0

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        val = self._db.get(name)
        if val is None:
            self._db[name] = value
        return val

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        try:
            self._db[name] = int(self._db.get(name, '0')) + amount
        except (TypeError, ValueError):
            raise redis.ResponseError("value is not an integer or out of "
                                      "range.")
        return self._db[name]

    def keys(self, pattern=None):
        return [key for key in self._db.keys()
                if not key or not pattern or fnmatch.fnmatch(key, pattern)]

    def mget(self, keys, *args):
        all_keys = self._list_or_args(keys, args)
        found = []
        for key in all_keys:
            found.append(self._db.get(key))
        return found

    def mset(self, mapping):
        for key, val in mapping.iteritems():
            self.set(key, val)
        return True

    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        if not any(k in self._db for k in mapping):
            for key, val in mapping.iteritems():
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
        if (not nx and not xx) \
        or (nx and self._db.get(name, None) is None) \
        or (xx and not self._db.get(name, None) is None):
            if ex > 0:
                self._db.expire(name, datetime.now() + timedelta(seconds=ex))
            elif px > 0:
                self._db.expire(name, datetime.now() + timedelta(milliseconds=px))
            self._db[name] = str(value)
            return True
        else:
            return None

    __setitem__ = set

    def setbit(self, name, offset, value):
        val = self._db.get(name, '\x00')
        byte = offset / 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        if len(val) - 1 < byte:
            # We need to expand val so that we can set the appropriate
            # bit.
            needed = byte - (len(val) - 1)
            val += '\x00' * needed
        if value == 1:
            new_byte = chr(ord(val[byte]) | (1 << actual_bitoffset))
        else:
            new_byte = chr(ord(val[byte]) ^ (1 << actual_bitoffset))
        reconstructed = list(val)
        reconstructed[byte] = new_byte
        self._db[name] = ''.join(reconstructed)

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
            return ''
    # Redis >= 2.0.0 this command is called getrange
    # according to the docs.
    getrange = substr

    def ttl(self, name):
        if name not in self._db:
            return None

        exp_time = self._db.expiring(name)
        if not exp_time:
            return None

        now = datetime.now()
        if now > exp_time:
            return None
        else:
            return round((exp_time - now).days * 3600 * 24
                         + (exp_time - now).seconds
                         + (exp_time - now).microseconds / 1E6)

    def type(self, name):
        pass

    def watch(self, *names):
        pass

    def unwatch(self):
        pass

    def delete(self, *names):
        deleted = 0
        for name in names:
            try:
                del self._db[name]
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
            if not (start is None and num is None):
                data = data[start:start + num]
            if desc:
                data = list(reversed(data))
            if store is not None:
                self._db[store] = data
                return len(data)
            else:
                return self._retrive_data_from_sort(data, get)
        except KeyError:
            return []

    def _retrive_data_from_sort(self, data, get):
        if get is not None:
            if isinstance(get, basestring):
                get = [get]
            new_data = []
            for k in data:
                for g in get:
                    single_item = self._get_single_item(k, g)
                    new_data.append(single_item)
            data = new_data
        return data

    def _get_single_item(self, k, g):
        if '*' in g:
            g = g.replace('*', k)
            if '->' in g:
                key, hash_key = g.split('->')
                single_item = self._db.get(key, {}).get(hash_key)
            else:
                single_item = self._db.get(g)
        elif '#' in g:
            single_item = k
        else:
            single_item = None
        return single_item

    def _strtod_key_func(self, arg):
        # str()'ing the arg is important! Don't ever remove this.
        arg = str(arg)
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
        def _by_key(arg):
            key = by.replace('*', arg)
            if '->' in by:
                key, hash_key = key.split('->')
                return self._db.get(key, {}).get(hash_key)
            else:
                return self._db.get(key)
        data.sort(key=_by_key)

    def lpush(self, name, *values):
        self._db.setdefault(name, [])[0:0] = list(reversed((values)))
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
        self._db.setdefault(name, []).extend([str(x) for x in values])
        return len(self._db[name])

    def lpop(self, name):
        try:
            return self._db.get(name, []).pop(0)
        except IndexError:
            return None

    def lset(self, name, index, value):
        try:
            self._db.get(name, [])[index] = value
        except IndexError:
            raise redis.ResponseError("index out of range")

    def rpushx(self, name, value):
        try:
            self._db[name].append(value)
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
            self._db[name].insert(0, value)
        except KeyError:
            return

    def rpop(self, name):
        try:
            return self._db.get(name, []).pop()
        except IndexError:
            return None

    def linsert(self, name, where, refvalue, value):
        index = self._db.get(name, []).index(refvalue)
        self._db.get(name, []).insert(index, value)

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
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        for key in keys:
            if self._db.get(key, []):
                return (key, self._db[key].pop(0))

    def brpop(self, keys, timeout=0):
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
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
        new = int(self._db.setdefault(name, _StrKeyDict()).get(key, '0')) + amount
        self._db[name][key] = new
        return new

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self._db.get(name, {}).keys()

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return len(self._db.get(name, {}))

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        key_is_new = key not in self._db.get(name, {})
        self._db.setdefault(name, _StrKeyDict())[key] = str(value)
        return 1 if key_is_new else 0

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        if key in self._db.get(name, {}):
            return False
        self._db.setdefault(name, _StrKeyDict())[key] = str(value)
        return True

    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """
        if not mapping:
            raise redis.DataError("'hmset' with 'mapping' of length 0")
        self._db.setdefault(name, _StrKeyDict()).update(mapping)
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
        a_set |= set(values)
        return len(a_set) - card

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return len(self._db.get(name, set()))

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        all_keys = self._list_or_args(keys, args)
        diff = self._db.get(all_keys[0], set()).copy()
        for key in all_keys[1:]:
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
        all_keys = self._list_or_args(keys, args)
        intersect = self._db.get(all_keys[0], set()).copy()
        for key in all_keys[1:]:
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
        return value in self._db.get(name, set())

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self._db.get(name, set())

    def smove(self, src, dst, value):
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
        a_set -= set(values)
        return card - len(a_set)

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        all_keys = self._list_or_args(keys, args)
        union = self._db.get(all_keys[0], set()).copy()
        for key in all_keys[1:]:
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
        zset = self._db.setdefault(name, _StrKeyDict())
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
        for score in self._db.get(name, {}).values():
            if min <= score <= max:
                found += 1
        return found

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        d = self._db.setdefault(name, _StrKeyDict())
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
        by_keyname = sorted(all_items.items(), key=lambda x: x[0])
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
        matches = []
        for item in in_order:
            if min <= all_items[item] <= max:
                matches.append(item)
        if start is not None:
            matches = matches[start:start + num]
        if withscores:
            return [(k, all_items[k]) for k in matches]
        return matches

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        all_items = self._db.get(name, {})
        in_order = sorted(all_items, key=lambda x: all_items[x])
        try:
            return in_order.index(value)
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
        removed = 0
        for key in all_items.copy():
            if min <= all_items[key] <= max:
                del all_items[key]
                removed += 1
        return removed

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
        new_zset = _StrKeyDict()
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
        if isinstance(keys, basestring):
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
        else:
            value = pairs.keys()[0]
            score = pairs.values()[0]
        self._db.setdefault(name, _StrKeyDict())[value] = score


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

    def execute(self):
        """Run all the commands in the pipeline and return the results."""
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
        ret = [getattr(self.owner, name)(*args, **kwargs)
               for name, args, kwargs in self.commands]
        self.commands = []
        self.watching = {}
        return ret

    def watch(self, *keys):
        self.watching.update((key, copy.deepcopy(self.owner._db.get(key)))
                             for key in keys)
        self.need_reset = True
        self.is_immediate = True

    def multi(self):
        self.is_immediate = False

    def reset(self):
        self.need_reset = False
