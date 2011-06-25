import random

import redis
import redis.client


class FakeRedis(object):
    def __init__(self):
        self._db = {}

    def flushdb(self):
        self._db = {}
        return True

    def keys(self):
        return self._db.keys()

    def get(self, name):
        return self._db.get(name)

    def set(self, name, value):
        self._db[name] = value
        return True

    def lpush(self, name, value):
        self._db.setdefault(name, []).insert(0, value)
        return len(self._db[name])

    def lrange(self, name, start, end):
        if end == -1:
            end = None
        else:
            end += 1
        return self._db.get(name, [])[start:end]

    def llen(self, name):
        return len(self._db.get(name, []))

    def lrem(self, name, value, count=0):
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

    def rpush(self, name, value):
        self._db.setdefault(name, []).append(value)
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
        raise NotImplementedError()

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
        el = self._db.get(src).pop()
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
        el = self._db.get(src).pop()
        try:
            self._db[dst].insert(0, el)
        except KeyError:
            self._db[dst] = [el]
        return el

    def hdel(self, name, key):
        try:
            del self._db.get(name, {})[key]
            return True
        except KeyError:
            return False

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
        return self._db.get(name, {})

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        new = self._db.setdefault(name, {}).get(key, 0) + amount
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
        self._db.setdefault(name, {})[key] = value
        return 1

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        if key in self._db.get(name, {}):
            return False
        self._db.setdefault(name, {})[key] = value
        return True

    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """
        if not mapping:
            raise redis.DataError("'hmset' with 'mapping' of length 0")
        self._db.setdefault(name, {}).update(mapping)
        return True

    def hmget(self, name, keys):
        "Returns a list of values ordered identically to ``keys``"
        h = self._db.get(name, {})
        return [h.get(k) for k in keys]

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self._db.get(name, {}).values()

    def sadd(self, name, value):
        "Add ``value`` to set ``name``"
        a_set = self._db.setdefault(name, set())
        if value in a_set:
            return False
        else:
            a_set.add(value)
            return True

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return len(self._db.get(name, set()))

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        all_keys = redis.client.list_or_args(keys, args)
        diff = self._db.get(all_keys[0], set())
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
        all_keys = redis.client.list_or_args(keys, args)
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
        return self._db.get(name)

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

    def srem(self, name, value):
        "Remove ``value`` from set ``name``"
        try:
            self._db.get(name, set()).remove(value)
            return True
        except KeyError:
            return False

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        all_keys = redis.client.list_or_args(keys, args)
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
