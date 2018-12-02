import os
import sys
import io
import queue
import time
import threading
import math
import random
import re
import warnings
import functools
from collections import defaultdict
try:
    # Python 3.8+ https://docs.python.org/3/whatsnew/3.7.html#id3
    from collections.abc import MutableMapping
except ImportError:
    # Python 2.6, 2.7
    from collections import MutableMapping

import redis
from redis._compat import nativestr   # TODO don't depend on private

from .zset import ZSet


DEFAULT_ENCODING = sys.getdefaultencoding()    # TODO: Python 2 support
MAX_STRING_SIZE = 512 * 1024 * 1024

INVALID_EXPIRE_MSG = "invalid expire time in {}"
WRONGTYPE_MSG = \
    "WRONGTYPE Operation against a key holding the wrong kind of value"
SYNTAX_ERROR_MSG = "syntax error"
INVALID_INT_MSG = "value is not an integer or out of range"
INVALID_FLOAT_MSG = "value is not a valid float"
INVALID_OFFSET_MSG = "offset is out of range"
INVALID_BIT_OFFSET_MSG = "bit offset is not an integer or out of range"
INVALID_DB_MSG = "DB index is out of range"
INVALID_MIN_MAX_FLOAT_MSG = "min or max is not a float"
INVALID_MIN_MAX_STR_MSG = "min or max not a valid string range item"
STRING_OVERFLOW_MSG = "string exceeds maximum allowed size (512MB)"
OVERFLOW_MSG = "increment or decrement would overflow"
NONFINITE_MSG = "increment would produce NaN or Infinity"
SRC_DST_SAME_MSG = "source and destination objects are the same"
NO_KEY_MSG = "no such key"
INDEX_ERROR_MSG = "index out of range"
WRONG_ARGS_MSG = "wrong number of arguments for '{}' command"
UNKNOWN_COMMAND_MSG = "unknown command '{}'"
MULTI_NESTED_MSG = "MULTI calls can not be nested"
WITHOUT_MULTI_MSG = "{} without MULTI"
WATCH_INSIDE_MULTI_MSG = "WATCH inside MULTI is not allowed"
OK = b'OK'
QUEUED = b'QUEUED'


# TODO: Python 2 support
def byte_to_int(b):
    assert isinstance(b, int)
    return b


def compile_pattern(pattern):
    """Compile a glob pattern (e.g. for keys) to a bytes regex.

    fnmatch.fnmatchcase doesn't work for this, because it uses different
    escaping rules to redis, uses ! instead of ^ to negate a character set,
    and handles invalid cases (such as a [ without a ]) differently. This
    implementation was written by studying the redis implementation.
    """
    # It's easier to work with text than bytes, because indexing bytes
    # doesn't behave the same in Python 3. Latin-1 will round-trip safely.
    pattern = pattern.decode('latin-1')
    parts = ['^']
    i = 0
    L = len(pattern)
    while i < L:
        c = pattern[i]
        i += 1
        if c == '?':
            parts.append('.')
        elif c == '*':
            parts.append('.*')
        elif c == '\\':
            if i == L:
                i -= 1
            parts.append(re.escape(pattern[i]))
            i += 1
        elif c == '[':
            parts.append('[')
            if i < L and pattern[i] == '^':
                i += 1
                parts.append('^')
            parts_len = len(parts)  # To detect if anything was added
            while i < L:
                if pattern[i] == '\\' and i + 1 < L:
                    i += 1
                    parts.append(re.escape(pattern[i]))
                elif pattern[i] == ']':
                    i += 1
                    break
                elif i + 2 < L and pattern[i + 1] == '-':
                    start = pattern[i]
                    end = pattern[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + '-' + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern[i]))
                i += 1
            if len(parts) == parts_len:
                if parts[-1] == '[':
                    # Empty group - will never match
                    parts[-1] = '(?:$.)'
                else:
                    # Negated empty group - matches any character
                    assert parts[-1] == '^'
                    parts.pop()
                    parts[-1] = '.'
            parts.append(']')
        else:
            parts.append(re.escape(c))
    parts.append('\\Z')
    regex = ''.join(parts).encode('latin-1')
    return re.compile(regex, re.S)


class Item(object):
    """An item stored in the database"""

    __slots__ = ['value', 'expireat']

    def __init__(self, value):
        self.value = value
        self.expireat = None


class CommandItem(object):
    """An item referenced by a command.

    It wraps an Item but has extra fields to manage updates and notifications.
    """
    def __init__(self, key, db, item=None, default=None):
        if item is None:
            self._value = default
            self.expireat = None
        else:
            self._value = item.value
            self.expireat = item.expireat
        self.key = key
        self.db = db
        self._modified = False

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value
        self._modified = True
        self.expireat = None

    def get(self, default):
        return self._value if self else default

    def update(self, new_value):
        self._value = new_value
        self._modified = True

    def updated(self):
        self._modified = True

    def writeback(self):
        if self._modified:
            self.db.notify_watch(self.key)
        if not isinstance(self.value, bytes) and not self.value:
            self.db.pop(self.key, None)
            return
        item = self.db.setdefault(self.key, Item(None))
        item.value = self.value
        item.expireat = self.expireat

    def __bool__(self):
        return bool(self._value) or isinstance(self._value, bytes)

    __nonzero__ = __bool__    # For Python 2


class Database(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self.time = 0.0
        self._watches = defaultdict(set)      # key to set of connections

    def swap(self, other):
        self._dict, other._dict = other._dict, self._dict
        self.time, other.time = other.time, self.time
        # TODO: should watches swap too?

    def notify_watch(self, key):
        for sock in self._watches.get(key, set()):
            sock.notify_watch()

    def add_watch(self, key, sock):
        self._watches[key].add(sock)

    def remove_watch(self, key, sock):
        watches = self._watches[key]
        watches.discard(sock)
        if not watches:
            del self._watches[key]

    def clear(self):
        for key in self:
            self.notify_watch(key)
        self._dict.clear()

    def expired(self, item):
        return item.expireat is not None and item.expireat < self.time

    def _remove_expired(self):
        for key in list(self._dict):
            item = self._dict[key]
            if self.expired(item):
                del self._dict[key]

    def __getitem__(self, key):
        item = self._dict[key]
        if self.expired(item):
            del self._dict[key]
            raise KeyError(key)
        return item

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __delitem__(self, key):
        del self._dict[key]

    def __iter__(self):
        self._remove_expired()
        return iter(self._dict)

    def __len__(self):
        self._remove_expired()
        return len(self._dict)


class Hash(dict):
    redis_type = b'hash'


class Int(object):
    """Argument converter for 64-bit signed integers"""

    DECODE_ERROR = INVALID_INT_MSG
    ENCODE_ERROR = OVERFLOW_MSG

    @classmethod
    def valid(cls, value):
        return -2**63 <= value < 2**63

    @classmethod
    def decode(cls, value):
        try:
            out = int(value)
            if not cls.valid(out) or str(out).encode() != value:
                raise ValueError
        except ValueError:
            raise redis.ResponseError(cls.DECODE_ERROR)
        return out

    @classmethod
    def encode(cls, value):
        if cls.valid(value):
            return str(value).encode()
        else:
            raise redis.ResponseError(cls.ENCODE_ERROR)


class BitOffset(Int):
    """Argument converter for unsigned bit positions"""

    DECODE_ERROR = INVALID_BIT_OFFSET_MSG

    @classmethod
    def valid(cls, value):
        return 0 <= value < 8 * MAX_STRING_SIZE     # Redis imposes 512MB limit on keys


class DbIndex(Int):
    """Argument converted for databased indices"""

    DECODE_ERROR = INVALID_DB_MSG

    @classmethod
    def valid(cls, value):
        return 0 <= value < 16


class Float(object):
    """Argument converter for floating-point values.

    Redis uses long double for some cases (INCRBYFLOAT, HINCRBYFLOAT)
    and double for others (zset scores), but Python doesn't support
    long double.
    """

    @classmethod
    def decode(cls, value):
        try:
            if value[:1].isspace():
                raise ValueError       # redis explicitly rejects this
            out = float(value)
            if math.isnan(out):
                raise ValueError
            # Values that over- or underflow- are explicitly rejected by
            # redis. This is a crude hack to determine whether the input
            # may have been such a value.
            if out in (math.inf, -math.inf, 0.0) and re.match(b'^[^a-zA-Z]*[1-9]', value):
                raise ValueError
            return out
        except ValueError:
            raise redis.ResponseError(INVALID_FLOAT_MSG)

    @classmethod
    def encode(cls, value, humanfriendly):
        if math.isinf(value):
            return str(value).encode()
        elif humanfriendly:
            # Algorithm from ld2string in redis
            out = '{:.17f}'.format(value)
            out = re.sub(r'(?:\.)?0+$', '', out)
            return out.encode()
        else:
            return '{:.17g}'.format(value).encode()


class ScoreTest(object):
    """Argument converter for sorted set score endpoints."""
    def __init__(self, value, exclusive=False):
        self.value = value
        self.exclusive = exclusive

    @classmethod
    def decode(cls, value):
        try:
            if value[:1] == b'(':
                return cls(Float.decode(value[1:]), True)
            else:
                return cls(Float.decode(value), False)
        except redis.ResponseError:
            raise redis.ResponseError(INVALID_MIN_MAX_FLOAT_MSG)

    @property
    def lower_bound(self):
        return (self.value, AfterAny() if self.exclusive else BeforeAny())

    @property
    def upper_bound(self):
        return (self.value, BeforeAny() if self.exclusive else AfterAny())


class StringTest(object):
    """Argument converter for sorted set LEX endpoints."""
    def __init__(self, value, exclusive):
        self.value = value
        self.exclusive = exclusive

    @classmethod
    def decode(cls, value):
        if value == b'-':
            return cls(BeforeAny(), True)
        elif value == b'+':
            return cls(AfterAny(), True)
        elif value[:1] == b'(':
            return cls(value[1:], True)
        elif value[:1] == b'[':
            return cls(value[1:], False)
        else:
            raise redis.ResponseError(INVALID_MIN_MAX_STR_MSG)


@functools.total_ordering
class BeforeAny(object):
    def __gt__(self, other):
        return False

    def __eq__(self, other):
        return isinstance(other, BeforeAny)


@functools.total_ordering
class AfterAny(object):
    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return isinstance(other, AfterAny)


class Key(object):
    """Marker to indicate that argument in signature is a key"""
    # TODO: add argument to specify a return value if the key is not found

    UNSPECIFIED = object()

    def __init__(self, type_=None, missing_return=UNSPECIFIED):
        self.type_ = type_
        self.missing_return = missing_return


class Signature(object):
    def __init__(self, name, fixed, repeat=()):
        self.name = name
        self.fixed = fixed
        self.repeat = repeat

    def check_arity(self, args):
        if len(args) != len(self.fixed):
            delta = len(args) - len(self.fixed)
            if delta < 0 or not self.repeat:
                raise redis.ResponseError(WRONG_ARGS_MSG.format(self.name))

    def apply(self, args, db):
        """Returns a tuple, which is either:
        - transformed args and a dict of CommandItems; or
        - a single containing a short-circuit return value
        """
        self.check_arity(args)
        if self.repeat:
            delta = len(args) - len(self.fixed)
            if delta % len(self.repeat) != 0:
                raise redis.ResponseError(WRONG_ARGS_MSG.format(self.name))

        types = list(self.fixed)
        for i in range(len(args) - len(types)):
            types.append(self.repeat[i % len(self.repeat)])

        args = list(args)
        # First pass: convert/validate non-keys, and short-circuit on missing keys
        for i, (arg, type_) in enumerate(zip(args, types)):
            if isinstance(type_, Key):
                if type_.missing_return is not Key.UNSPECIFIED and arg not in db:
                    return (type_.missing_return,)
            elif type_ != bytes:
                args[i] = type_.decode(args[i])

        # Second pass: read keys and check their types
        command_items = {}
        for i, (arg, type_) in enumerate(zip(args, types)):
            if isinstance(type_, Key):
                item = db.get(arg)
                default = None
                if type_.type_ is not None:
                    if item is not None and type(item.value) != type_.type_:
                        raise redis.ResponseError(WRONGTYPE_MSG)
                    if item is None:
                        if type_.type_ is not bytes:
                            default = type_.type_()
                args[i] = command_items[arg] = CommandItem(arg, db, item, default=default)

        return args, command_items


def command(*args, **kwargs):
    def decorator(func):
        name = kwargs.pop('name', func.__name__)
        func._fakeredis_sig = Signature(name, *args, **kwargs)
        return func

    return decorator


class FakeServer(object):
    def __init__(self):
        self.dbs = defaultdict(Database)
        self.lock = threading.Lock()


class FakeSocket(object):
    def __init__(self, server):
        self._server = server
        self._db = server.dbs[0]
        self._db_num = 0
        # When in a MULTI, set to a list of function calls
        self._transaction = None
        self._transaction_failed = False
        self._watch_notified = False
        self._watches = set()
        self.responses = queue.Queue()

    def shutdown(self, flags):
        pass     # For compatibility with socket.socket

    def close(self):
        # TODO: unsubscribe from pub/sub
        with self._server.lock:
            self._clear_watches()
        self._server = None
        self._db = None
        self.responses = None

    @staticmethod
    def _parse_packed_command(command):
        fp = io.BytesIO(command)
        line = fp.readline()
        assert line[:1] == b'*'        # array
        assert line[-2:] == b'\r\n'
        n_fields = int(line[1:-2])
        fields = []
        for i in range(n_fields):
            line = fp.readline()
            assert line[:1] == b'$'    # string
            assert line[-2:] == b'\r\n'
            length = int(line[1:-2])
            fields.append(fp.read(length))
            fp.read(2)                 # CRLF
        return fields

    def _run_command(self, func, sig, args):
        command_items = {}
        try:
            ret = sig.apply(args, self._db)
            if len(ret) == 1:
                result = ret[0]
            else:
                args, command_items = ret
                result = func(*args)
        except redis.ResponseError as exc:
            result = exc
        for command_item in command_items.values():
            command_item.writeback()
        return result

    def sendall(self, command):
        try:
            if isinstance(command, list):
                command = b''.join(command)
            fields = self._parse_packed_command(command)
            if not fields:
                return
            name = fields[0]
            # redis treats the command as NULL-terminated
            if b'\0' in name:
                name = name[:name.find(b'\0')]
            name = nativestr(name)
            func_name = name.lower()
            func = getattr(self, func_name, None)
            if name.startswith('_') or not func or not hasattr(func, '_fakeredis_sig'):
                # redis remaps \r or \n in an error to ' ' to make it legal protocol
                clean_name = name.replace('\r', ' ').replace('\n', ' ')
                raise redis.ResponseError(UNKNOWN_COMMAND_MSG.format(clean_name))
            sig = func._fakeredis_sig
            with self._server.lock:
                now = time.time()
                for db in self._server.dbs.values():
                    db.time = now
                sig.check_arity(fields[1:])
                if self._transaction is not None \
                        and func_name not in ('exec', 'discard', 'multi', 'watch'):
                    self._transaction.append((func, sig, fields[1:]))
                    result = QUEUED
                else:
                    result = self._run_command(func, sig, fields[1:])
                # TODO: decode results if requested
        except redis.ResponseError as exc:
            if self._transaction is not None:
                # TODO: should not apply if the exception is from _run_command
                # e.g. watch inside multi
                self._transaction_failed = True
            result = exc
        self.responses.put(result)

    def notify_watch(self):
        self._watch_notified = True

    @staticmethod
    def _fix_range(start, end, length):
        # Negative number handling is based on the redis source code
        if start < 0 and end < 0 and start > end:
            return -1, -1
        if start < 0:
            start = max(0, start + length)
        if end < 0:
            end = max(0, end + length)
        end = min(end, length - 1)
        return start, end + 1

    @staticmethod
    def _fix_range_zset(start, end, length):
        # Redis handles negative slightly differently for zrange
        if start < 0:
            start = max(0, start + length)
        if end < 0:
            end += length
        if start > end or start >= length:
            return -1, -1
        end = min(end, length - 1)
        return start, end + 1

    # Connection commands
    # TODO: auth, quit

    @command((bytes,))
    def echo(self, message):
        return message

    @command((), (bytes,))
    def ping(self, *args):
        # TODO: behaves differently on a pubsub connection
        if len(args) == 0:
            return "PONG"
        elif len(args) == 1:
            return args[0]
        else:
            raise redis.ResponseError(WRONG_ARGS_MSG)

    @command((DbIndex,))
    def select(self, index):
        self._db = self._server.dbs[index]
        self._db_num = index
        return OK

    @command((DbIndex, DbIndex))
    def swapdb(self, index1, index2):
        if index1 != index2:
            db1 = self._server.dbs[index1]
            db2 = self._server.dbs[index2]
            db1.swap(db2)
        return OK

    # Key commands
    # TODO: lots

    @command((Key(),), name='del')
    def delete(self, key):
        if not key:
            return 0
        key.value = None
        return 1

    @command((Key(),))
    def exists(self, key):
        return 1 if key else 0

    def _expireat(self, key, timestamp):
        if not key:
            return 0
        else:
            key.expireat = timestamp
            return 1

    def _ttl(self, key, scale):
        if not key:
            return -2
        elif key.expireat is None:
            return -1
        else:
            return int(round((key.expireat - self._db.time) * scale))

    @command((Key(), Int))
    def expire(self, key, seconds):
        return self._expireat(key, self._db.time + seconds)

    @command((Key(), Int))
    def expireat(self, key, timestamp):
        return self._expireat(key, float(timestamp))

    @command((Key(), Int))
    def pexpire(self, key, ms):
        return self._expireat(key, self._db.time + ms / 1000.0)

    @command((Key(), Int))
    def pexpireat(self, key, ms_timestamp):
        return self._expireat(key, ms_timestamp / 1000.0)

    @command((Key(),))
    def ttl(self, key):
        return self._ttl(key, 1.0)

    @command((Key(),))
    def pttl(self, key):
        return self._ttl(key, 1000.0)

    @command((Key(),))
    def persist(self, key):
        if key.expireat is None:
            return 0
        key.expireat = None
        # TODO: does this mark it modified for WATCH?
        return 1

    @command((bytes,))
    def keys(self, pattern):
        if pattern == b'*':
            return list(self._db)
        else:
            regex = compile_pattern(pattern)
            return [key for key in self._db if regex.match(key)]

    @command((Key(), DbIndex))
    def move(self, key, db):
        if db == self._db_num:
            raise redis.ResponseError(SRC_DST_SAME_MSG)
        if not item or key.key in self._server.dbs[db]:
            return 0
        # TODO: what is the interaction with expiry and WATCH?
        self._server.dbs[db][key] = key.item
        key.value = None   # Causes deletion
        return 1

    @command(())
    def randomkey(self):
        keys = list(self._db.keys())
        if not keys:
            return None
        return random.choice(keys)

    @command((Key(), Key()))
    def rename(self, key, newkey):
        if not key:
            raise redis.ResponseError(NO_KEY_MSG)
        # TODO: check interaction with WATCH
        if newkey.key != key.key:
            newkey.value = key.value
            newkey.expireat = key.expireat
            key.value = None
        return OK

    @command((Key(), Key()))
    def renamenx(self, key, newkey):
        if not key:
            raise redis.ResponseError(NO_KEY_MSG)
        if newkey:
            return 0
        self.rename(key, newkey)
        return 1

    # Transaction commands

    def _clear_watches(self):
        self._watch_notified = False
        while self._watches:
            (key, db) = self._watches.pop()
            db.remove_watch(key, self)

    @command(())
    def multi(self):
        if self._transaction is not None:
            raise redis.ResponseError(MULTI_NESTED_MSG)
        self._transaction = []
        self._transaction_failed = False

    @command(())
    def discard(self):
        if self._transaction is None:
            raise redis.ResponseError(WITHOUT_MULTI_MSG.format('DISCARD'))
        self._transaction = None
        self._transaction_failed = False
        self._clear_watches()

    @command(())
    def exec(self):
        if self._transaction is None:
            raise redis.ResponseError(WITHOUT_MULTI_MSG.format('EXEC'))
        if self._transaction_failed:
            self._transaction = None
            raise redis.ResponseError(EXECABORT_MSG)
        transaction = self._transaction
        self._transaction = None
        self._transaction_failed = False
        watch_notified = self._watch_notified
        self._clear_watches()
        if watch_notified:
            return None
        result = []
        for func, sig, args in transaction:
            try:
                ans = self._run_command(func, sig, args)
            except redis.ResponseError as exc:
                ans = exc
            result.append(ans)
        return result

    @command((Key(),), (Key(),))
    def watch(self, *keys):
        if self._transaction is not None:
            raise redis.ResponseError(WATCH_INSIDE_MULTI_MSG)
        for key in keys:
            if key not in self._watches:
                self._watches.add(key)
                self._db.add_watch(key, self)
        return OK

    @command(())
    def unwatch(self):
        self._clear_watches()
        return OK

    # String commands
    # TODO: bitfield, bitop, bitpos, mset*, psetex, setex, setnx, setrange, strlen

    @command((Key(bytes), bytes))
    def append(self, key, value):
        old = key.get(b'')
        if len(old) + len(value) > MAX_STRING_SIZE:
            raise redis.ResponseError(STRING_OVERFLOW_MSG)
        key.update(key.get(b'') + value)
        return len(key.value)

    @command((Key(bytes, 0),), (bytes,))
    def bitcount(self, key, *args):
        # Redis checks the argument count before decoding integers. That's why
        # we can't declare them as Int.
        if not key:
            return 0
        if args:
            if len(args) != 2:
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
            start = Int.decode(args[0])
            end = Int.decode(args[1])
            start, end = self._fix_range(start, end, len(key.value))
            value = key.value[start:end]
        else:
            value = key.value
        return sum([bin(byte_to_int(l)).count('1') for l in value])

    @command((Key(bytes), Int))
    def decrby(self, key, amount):
        return self.incrby(key, -amount)

    @command((Key(bytes),))
    def decr(self, key):
        return self.incrby(key, -1)

    @command((Key(bytes), Int))
    def incrby(self, key, amount):
        c = Int.decode(key.get(b'0')) + amount
        key.update(Int.encode(c))
        return c

    @command((Key(bytes),))
    def incr(self, key):
        return self.incrby(key, 1)

    @command((Key(bytes), bytes))
    def incrbyfloat(self, key, amount):
        # TODO: introduce convert_order so that we can specify amount is Float
        c = Float.decode(key.get(b'0')) + Float.decode(amount)
        if not math.isfinite(c):
            raise redis.ResponseError(NONFINITE_MSG)
        encoded = Float.encode(c, True)
        key.update(encoded)
        return encoded

    @command((Key(bytes),))
    def get(self, key):
        return key.get(None)

    @command((Key(bytes), BitOffset))
    def getbit(self, key, offset):
        value = key.get(b'')
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        try:
            actual_val = byte_to_int(value[byte])
        except IndexError:
            return 0
        return 1 if (1 << actual_bitoffset) & actual_val else 0

    @command((Key(bytes), Int, Int))
    def getrange(self, key, start, end):
        value = key.get(b'')
        start, end = self._fix_range(start, end, len(value))
        return value[start:end]

    # substr is a deprecated alias for getrange
    @command((Key(bytes), Int, Int))
    def substr(self, key, start, end):
        return self.getrange(key, start, end)

    @command((Key(bytes), bytes))
    def getset(self, key, value):
        old = key.value
        key.value = value
        return old

    @command((Key(),), (Key(),))
    def mget(self, *keys):
        return [key.value if isinstance(key.value, bytes) else None for key in keys]

    @command((Key(), bytes), (Key(), bytes))
    def mset(self, *args):
        for i in range(0, len(args), 2):
            args[i].value = args[i + 1]
        return OK

    @command((Key(), bytes), (Key(), bytes))
    def msetnx(self, *args):
        for i in range(0, len(args), 2):
            if args[i]:
                return 0
        for i in range(0, len(args), 2):
            args[i].value = args[i + 1]
        return 1

    @command((Key(), bytes), (bytes,))
    def set(self, key, value, *args):
        i = 0
        ex = None
        px = None
        xx = False
        nx = False
        while i < len(args):
            if args[i].lower() == b'nx':
                nx = True
                i += 1
            elif args[i].lower() == b'xx':
                xx = True
                i += 1
            elif args[i].lower() == b'ex' and i + 1 < len(args):
                ex = Int.decode(args[i + 1])
                if ex <= 0:
                    raise redis.ResponseError(INVALID_EXPIRE_MSG.format('set'))
                i += 2
            elif args[i].lower() == b'px' and i + 1 < len(args):
                px = Int.decode(args[i + 1])
                if px <= 0:
                    raise redis.ResponseError(INVALID_EXPIRE_MSG.format('set'))
                i += 2
            else:
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
        if (xx and nx) or (px is not None and ex is not None):
            raise redis.ResponseError(SYNTAX_ERROR_MSG)

        if nx and key:
            return None
        if xx and not key:
            return None
        key.value = value
        if ex is not None:
            key.expireat = self._db.time + ex
        if px is not None:
            key.expireat = self._db.time + px / 1000.0
        return OK

    @command((Key(), Int, bytes))
    def setex(self, key, seconds, value):
        if seconds <= 0:
            raise redis.ResponseError(INVALID_EXPIRE_MSG.format('setex'))
        key.value = value
        key.expireat = self._db.time + seconds
        return OK

    @command((Key(), Int, bytes))
    def psetex(self, key, ms, value):
        if ms <= 0:
            raise redis.ResponseError(INVALID_EXPIRE_MSG.format('psetex'))
        key.value = value
        key.expireat = self._db.time + ms / 1000.0
        return OK

    @command((Key(), bytes))
    def setnx(self, key, value):
        if key:
            return 0
        key.value = value
        return 1

    @command((Key(bytes), Int, bytes))
    def setrange(self, key, offset, value):
        if offset < 0:
            raise redis.ResponseError(INVALID_OFFSET_MSG)
        elif not value:
            return len(key.get(b''))
        elif offset + len(value) > MAX_STRING_SIZE:
            raise redis.ResponseError(STRING_OVERFLOW_MSG)
        else:
            out = key.get(b'')
            if len(out) < offset:
                out += b'\x00' * (offset - len(out))
            out = out[0:offset] + value + out[offset+len(value):]
            key.update(out)
            return len(out)

    @command((Key(bytes),))
    def strlen(self, key):
        return len(key.get(b''))

    # Hash commands

    @command((Key(Hash), bytes), (bytes,))
    def hdel(self, key, *fields):
        h = key.value
        rem = 0
        for field in fields:
            if field in h:
                del h[field]
                key.updated()
                rem += 1
        return rem

    @command((Key(Hash), bytes))
    def hget(self, key, field):
        return key.value.get(field)

    @command((Key(Hash), bytes, bytes))
    def hset(self, key, field, value):
        h = key.value
        is_new = field not in h
        h[field] = value
        key.updated()
        return 1 if is_new else 0

    # List commands
    # TODO: blocking commands

    @command((Key(list, None), Int))
    def lindex(self, key, index):
        try:
            return key.value[index]
        except IndexError:
            return None

    @command((Key(list), bytes, bytes, bytes))
    def linsert(self, key, where, pivot, value):
        if where.lower() not in (b'before', b'after'):
            raise redis.ResponseError(SYNTAX_ERROR_MSG)
        if not key:
            return 0
        else:
            try:
                index = key.value.index(pivot)
            except ValueError:
                return -1
            if where.lower() == b'after':
                index += 1
            key.value.insert(index, value)
            key.updated()
            return len(key.value)

    @command((Key(list),))
    def llen(self, key):
        return len(key.value)

    @command((Key(list),))
    def lpop(self, key):
        try:
            ret = key.value.pop(0)
            key.updated()
            return ret
        except IndexError:
            return None

    @command((Key(list), bytes), (bytes,))
    def lpush(self, key, *values):
        for value in values:
            key.value.insert(0, value)
        key.updated()
        return len(key.value)

    @command((Key(list), bytes), (bytes,))
    def lpushx(self, key, *values):
        if not key:
            return 0
        return self.lpush(key, *values)

    @command((Key(list), Int, Int))
    def lrange(self, key, start, stop):
        start, stop = self._fix_range(start, stop, len(key.value))
        return key.value[start:stop]

    @command((Key(list), Int, bytes))
    def lrem(self, key, count, value):
        a_list = key.value
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
        if indices_to_remove:
            key.updated()
        return len(indices_to_remove)

    @command((Key(list), Int, bytes))
    def lset(self, key, index, value):
        if not key:
            raise redis.ResponseError(NO_KEY_MSG)
        try:
            key.value[index] = value
            key.updated()
        except IndexError:
            raise redis.ResponseError(INDEX_ERROR_MSG)
        return OK

    @command((Key(list), Int, Int))
    def ltrim(self, key, start, stop):
        if key:
            if stop == -1:
                stop = None
            else:
                stop += 1
            new_value = key.value[start:stop]
            if len(new_value) != len(key.value):
                key.update(new_value)
        return OK

    @command((Key(list),))
    def rpop(self, key):
        try:
            ret = key.value.pop()
            key.updated()
            return ret
        except IndexError:
            return None

    @command((Key(list, None), Key(list)))
    def rpoplpush(self, src, dst):
        el = self.rpop(src)
        self.lpush(dst, el)
        return el

    @command((Key(list), bytes), (bytes,))
    def rpush(self, key, *values):
        for value in values:
            key.value.append(value)
        key.updated()
        return len(key.value)

    @command((Key(list), bytes), (bytes,))
    def rpushx(self, key, *values):
        if not key:
            return 0
        return self.rpush(key, *values)

    # Sorted set commands
    # TODO: blocking commands, set operations, zpopmin/zpopmax
    @command((Key(ZSet), bytes, bytes), (bytes,))
    def zadd(self, key, *args):
        # TODO: handle NX, XX, CH, INCR
        if len(args) % 2 != 0:
            raise redis.ResponseError(SYNTAX_ERROR_MSG)
        items = []
        # Parse all scores first, before updating
        for i in range(0, len(args), 2):
            score = Float.decode(args[i])
            items.append((score, args[i + 1]))
        old_len = len(key.value)
        for item in items:
            key.value[item[1]] = item[0]
            key.updated()
        return len(key.value) - old_len

    @command((Key(ZSet),))
    def zcard(self, key):
        return len(key.value)

    @command((Key(ZSet), ScoreTest, ScoreTest))
    def zcount(self, key, min, max):
        return key.value.zcount(min.lower_bound, max.upper_bound)

    @command((Key(ZSet), Float, bytes))
    def zincrby(self, key, increment, member):
        score = key.value.get(member, 0.0) + increment
        key.value[member] = score
        key.updated()
        return Float.encode(score, False)

    @command((Key(ZSet), StringTest, StringTest))
    def zlexcount(self, key, min, max):
        return key.value.zlexcount(min.value, min.exclusive, max.value, max.exclusive)

    def _zrange(self, key, start, stop, reverse, *args):
        zset = key.value
        if len(args) > 1 or (args and args[0].lower() != b'withscores'):
            raise redis.ResponseError(SYNTAX_ERROR_MSG)
        start, stop = self._fix_range_zset(start, stop, len(zset))
        if reverse:
            start, stop = len(zset) - stop, len(zset) - start
        items = zset.islice_score(start, stop, reverse)
        if args:
            out = []
            for item in items:
                out.append(item[1])
                out.append(item[0])
        else:
            out = [item[1] for item in items]
        return out

    @command((Key(ZSet), Int, Int), (bytes,))
    def zrange(self, key, start, stop, *args):
        return self._zrange(key, start, stop, False, *args)

    @command((Key(ZSet), Int, Int), (bytes,))
    def zrevrange(self, key, start, stop, *args):
        return self._zrange(key, start, stop, True, *args)

    def _zrangebylex(self, key, min, max, reverse, *args):
        if args:
            if len(args) != 3 or args[0].lower() != b'limit':
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
            offset = Int.decode(args[1])
            count = Int.decode(args[2])
        else:
            offset = 0
            count = -1
        zset = key.value
        items = zset.irange_lex(min.value, max.value,
                                inclusive=(not min.exclusive, not max.exclusive),
                                reverse=reverse)
        out = []
        for item in items:
            if offset:    # Note: not offset > 0, in order to match redis
                offset -= 1
                continue
            if count == 0:
                break
            count -= 1
            out.append(item)
        return out

    @command((Key(ZSet), StringTest, StringTest), (bytes,))
    def zrangebylex(self, key, min, max, *args):
        return self._zrangebylex(key, min, max, False, *args)

    @command((Key(ZSet), StringTest, StringTest), (bytes,))
    def zrevrangebylex(self, key, max, min, *args):
        return self._zrangebylex(key, min, max, True, *args)

    # Server commands
    # TODO: lots

    @command((), (bytes,))
    def flushdb(self, *args):
        if args:
            if len(args) != 1 or args[0].lower() != b'async':
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
        self._db.clear()
        return OK

    @command((), (bytes,))
    def flushall(self, *args):
        if args:
            if len(args) != 1 or args[0].lower() != b'async':
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
        for db in self._server.dbs.values():
            db.clear()
        # TODO: clear watches and/or pubsub as well?
        return OK


setattr(FakeSocket, 'del', FakeSocket.delete)
delattr(FakeSocket, 'delete')


class _DummyParser(object):
    def on_disconnect(self):
        pass

    def on_connect(self, connection):
        pass


class FakeConnection(redis.Connection):
    description_format = "FakeConnection<db=%(db)s>"

    def __init__(self, server, db=0, password=None,
                 encoding='utf-8', encoding_errors='strict',
                 decode_responses=False):
        self.pid = os.getpid()
        self.db = db
        self.password = password
        self.encoder = redis.connection.Encoder(encoding, encoding_errors, decode_responses)
        self._description_args = {'db': self.db}
        self._connect_callbacks = []
        self._buffer_cutoff = 6000
        self._server = FakeServer()
        # self._parser isn't used for anything, but some of the
        # base class methods depend on it and it's easier not to
        # override them.
        self._parser = _DummyParser()
        self._sock = None

    def _connect(self):
        return FakeSocket(self._server)

    def can_read(self, timeout=0):
        # TODO: handle timeout (needed for pub/sub)
        if not self._sock:
            self.connect()
        return bool(self._sock.responses.qsize())

    def read_response(self):
        response = self._sock.responses.get()
        if isinstance(response, redis.ResponseError):
            raise response
        return response


class FakeRedisMixin(object):
    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
                 ssl=False, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs=None, ssl_ca_certs=None,
                 max_connections=None, server=None):
        if not connection_pool:
            # Adapted from redis-py
            if charset is not None:
                warnings.warn(DeprecationWarning(
                    '"charset" is deprecated. Use "encoding" instead'))
                encoding = charset
            if errors is not None:
                warnings.warn(DeprecationWarning(
                    '"errors" is deprecated. Use "encoding_errors" instead'))
                encoding_errors = errors

            if server is None:
                server = FakeServer()
            kwargs = {
                'db': db,
                'password': password,
                'encoding': encoding,
                'encoding_errors': encoding_errors,
                'decode_responses': decode_responses,
                'max_connections': max_connections,
                'connection_class': FakeConnection,
                'server': server
            }
            connection_pool = redis.connection.ConnectionPool(**kwargs)
        super(FakeRedisMixin, self).__init__(
            host, port, db, password, socket_timeout, socket_connect_timeout,
            socket_keepalive, socket_keepalive_options, connection_pool,
            unix_socket_path, encoding, encoding_errors, charset, errors,
            decode_responses, retry_on_timeout,
            ssl, ssl_keyfile, ssl_certfile, ssl_cert_reqs, ssl_ca_certs,
            max_connections)


class FakeStrictRedis(FakeRedisMixin, redis.StrictRedis):
    pass


class FakeRedis(FakeRedisMixin, redis.Redis):
    pass
