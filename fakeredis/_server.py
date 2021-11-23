import functools
import hashlib
import inspect
import itertools
import logging
import math
import pickle
import queue
import random
import re
import threading
import time
import warnings
import weakref
from collections import defaultdict
from collections.abc import MutableMapping

import redis
import six

from ._zset import ZSet

LOGGER = logging.getLogger('fakeredis')
REDIS_LOG_LEVELS = {
    b'LOG_DEBUG': 0,
    b'LOG_VERBOSE': 1,
    b'LOG_NOTICE': 2,
    b'LOG_WARNING': 3
}
REDIS_LOG_LEVELS_TO_LOGGING = {
    0: logging.DEBUG,
    1: logging.INFO,
    2: logging.INFO,
    3: logging.WARNING
}

MAX_STRING_SIZE = 512 * 1024 * 1024

INVALID_EXPIRE_MSG = "ERR invalid expire time in {}"
WRONGTYPE_MSG = \
    "WRONGTYPE Operation against a key holding the wrong kind of value"
SYNTAX_ERROR_MSG = "ERR syntax error"
INVALID_INT_MSG = "ERR value is not an integer or out of range"
INVALID_FLOAT_MSG = "ERR value is not a valid float"
INVALID_OFFSET_MSG = "ERR offset is out of range"
INVALID_BIT_OFFSET_MSG = "ERR bit offset is not an integer or out of range"
INVALID_BIT_VALUE_MSG = "ERR bit is not an integer or out of range"
INVALID_DB_MSG = "ERR DB index is out of range"
INVALID_MIN_MAX_FLOAT_MSG = "ERR min or max is not a float"
INVALID_MIN_MAX_STR_MSG = "ERR min or max not a valid string range item"
STRING_OVERFLOW_MSG = "ERR string exceeds maximum allowed size (512MB)"
OVERFLOW_MSG = "ERR increment or decrement would overflow"
NONFINITE_MSG = "ERR increment would produce NaN or Infinity"
SCORE_NAN_MSG = "ERR resulting score is not a number (NaN)"
INVALID_SORT_FLOAT_MSG = "ERR One or more scores can't be converted into double"
SRC_DST_SAME_MSG = "ERR source and destination objects are the same"
NO_KEY_MSG = "ERR no such key"
INDEX_ERROR_MSG = "ERR index out of range"
ZADD_NX_XX_ERROR_MSG = "ERR ZADD allows either 'nx' or 'xx', not both"
ZADD_INCR_LEN_ERROR_MSG = "ERR INCR option supports a single increment-element pair"
ZUNIONSTORE_KEYS_MSG = "ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"
WRONG_ARGS_MSG = "ERR wrong number of arguments for '{}' command"
UNKNOWN_COMMAND_MSG = "ERR unknown command '{}'"
EXECABORT_MSG = "EXECABORT Transaction discarded because of previous errors."
MULTI_NESTED_MSG = "ERR MULTI calls can not be nested"
WITHOUT_MULTI_MSG = "ERR {0} without MULTI"
WATCH_INSIDE_MULTI_MSG = "ERR WATCH inside MULTI is not allowed"
NEGATIVE_KEYS_MSG = "ERR Number of keys can't be negative"
TOO_MANY_KEYS_MSG = "ERR Number of keys can't be greater than number of args"
TIMEOUT_NEGATIVE_MSG = "ERR timeout is negative"
NO_MATCHING_SCRIPT_MSG = "NOSCRIPT No matching script. Please use EVAL."
GLOBAL_VARIABLE_MSG = "ERR Script attempted to set global variables: {}"
COMMAND_IN_SCRIPT_MSG = "ERR This Redis command is not allowed from scripts"
BAD_SUBCOMMAND_MSG = "ERR Unknown {} subcommand or wrong # of args."
BAD_COMMAND_IN_PUBSUB_MSG = \
    "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"
CONNECTION_ERROR_MSG = "FakeRedis is emulating a connection error."
REQUIRES_MORE_ARGS_MSG = "ERR {} requires {} arguments or more."
LOG_INVALID_DEBUG_LEVEL_MSG = "ERR Invalid debug level."
LUA_COMMAND_ARG_MSG = "ERR Lua redis() command arguments must be strings or integers"
LUA_WRONG_NUMBER_ARGS_MSG = "ERR wrong number or type of arguments"
SCRIPT_ERROR_MSG = "ERR Error running script (call to f_{}): @user_script:?: {}"
RESTORE_KEY_EXISTS = "BUSYKEY Target key name already exists."
RESTORE_INVALID_CHECKSUM_MSG = "ERR DUMP payload version or checksum are wrong"
RESTORE_INVALID_TTL_MSG = "ERR Invalid TTL value, must be >= 0"

FLAG_NO_SCRIPT = 's'      # Command not allowed in scripts

# This needs to be grabbed early to avoid breaking tests that mock redis.Redis.
_ORIG_SIG = inspect.signature(redis.Redis)


class SimpleString:
    def __init__(self, value):
        assert isinstance(value, bytes)
        self.value = value


class SimpleError(Exception):
    """Exception that will be turned into a frontend-specific exception."""

    def __init__(self, value):
        assert isinstance(value, str)
        self.value = value


class NoResponse:
    """Returned by pub/sub commands to indicate that no response should be returned"""
    pass


OK = SimpleString(b'OK')
QUEUED = SimpleString(b'QUEUED')
PONG = SimpleString(b'PONG')
BGSAVE_STARTED = SimpleString(b'Background saving started')


def null_terminate(s):
    # Redis uses C functions on some strings, which means they stop at the
    # first NULL.
    if b'\0' in s:
        return s[:s.find(b'\0')]
    return s


def casenorm(s):
    return null_terminate(s).lower()


def casematch(a, b):
    return casenorm(a) == casenorm(b)


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
            else:
                parts.append(']')
        else:
            parts.append(re.escape(c))
    parts.append('\\Z')
    regex = ''.join(parts).encode('latin-1')
    return re.compile(regex, re.S)


class Item:
    """An item stored in the database"""

    __slots__ = ['value', 'expireat']

    def __init__(self, value):
        self.value = value
        self.expireat = None


class CommandItem:
    """An item referenced by a command.

    It wraps an Item but has extra fields to manage updates and notifications.
    """
    def __init__(self, key, db, item=None, default=None):
        if item is None:
            self._value = default
            self._expireat = None
        else:
            self._value = item.value
            self._expireat = item.expireat
        self.key = key
        self.db = db
        self._modified = False
        self._expireat_modified = False

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value
        self._modified = True
        self.expireat = None

    @property
    def expireat(self):
        return self._expireat

    @expireat.setter
    def expireat(self, value):
        self._expireat = value
        self._expireat_modified = True
        self._modified = True  # Since redis 6.0.7

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
            else:
                item = self.db.setdefault(self.key, Item(None))
                item.value = self.value
                item.expireat = self.expireat
        elif self._expireat_modified and self.key in self.db:
            self.db[self.key].expireat = self.expireat

    def __bool__(self):
        return bool(self._value) or isinstance(self._value, bytes)

    __nonzero__ = __bool__    # For Python 2


class Database(MutableMapping):
    def __init__(self, lock, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self.time = 0.0
        self._watches = defaultdict(weakref.WeakSet)      # key to set of connections
        self.condition = threading.Condition(lock)
        self._change_callbacks = set()

    def swap(self, other):
        self._dict, other._dict = other._dict, self._dict
        self.time, other.time = other.time, self.time

    def notify_watch(self, key):
        for sock in self._watches.get(key, set()):
            sock.notify_watch()
        self.condition.notify_all()
        for callback in self._change_callbacks:
            callback()

    def add_watch(self, key, sock):
        self._watches[key].add(sock)

    def remove_watch(self, key, sock):
        watches = self._watches[key]
        watches.discard(sock)
        if not watches:
            del self._watches[key]

    def add_change_callback(self, callback):
        self._change_callbacks.add(callback)

    def remove_change_callback(self, callback):
        self._change_callbacks.remove(callback)

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

    def __hash__(self):
        return hash(super(object, self))

    def __eq__(self, other):
        return super(object, self) == other


class Hash(dict):
    redis_type = b'hash'


class Int:
    """Argument converter for 64-bit signed integers"""

    DECODE_ERROR = INVALID_INT_MSG
    ENCODE_ERROR = OVERFLOW_MSG
    MIN_VALUE = -2**63
    MAX_VALUE = 2**63 - 1

    @classmethod
    def valid(cls, value):
        return cls.MIN_VALUE <= value <= cls.MAX_VALUE

    @classmethod
    def decode(cls, value):
        try:
            out = int(value)
            if not cls.valid(out) or str(out).encode() != value:
                raise ValueError
        except ValueError:
            raise SimpleError(cls.DECODE_ERROR)
        return out

    @classmethod
    def encode(cls, value):
        if cls.valid(value):
            return str(value).encode()
        else:
            raise SimpleError(cls.ENCODE_ERROR)


class BitOffset(Int):
    """Argument converter for unsigned bit positions"""

    DECODE_ERROR = INVALID_BIT_OFFSET_MSG
    MIN_VALUE = 0
    MAX_VALUE = 8 * MAX_STRING_SIZE - 1     # Redis imposes 512MB limit on keys


class BitValue(Int):
    DECODE_ERROR = INVALID_BIT_VALUE_MSG
    MIN_VALUE = 0
    MAX_VALUE = 1


class DbIndex(Int):
    """Argument converter for database indices"""

    DECODE_ERROR = INVALID_DB_MSG
    MIN_VALUE = 0
    MAX_VALUE = 15


class Timeout(Int):
    """Argument converter for timeouts"""

    DECODE_ERROR = TIMEOUT_NEGATIVE_MSG
    MIN_VALUE = 0


class Float:
    """Argument converter for floating-point values.

    Redis uses long double for some cases (INCRBYFLOAT, HINCRBYFLOAT)
    and double for others (zset scores), but Python doesn't support
    long double.
    """

    DECODE_ERROR = INVALID_FLOAT_MSG

    @classmethod
    def decode(cls, value,
               allow_leading_whitespace=False,
               allow_erange=False,
               allow_empty=False,
               crop_null=False):
        # redis has some quirks in float parsing, with several variants.
        # See https://github.com/antirez/redis/issues/5706
        try:
            if crop_null:
                value = null_terminate(value)
            if allow_empty and value == b'':
                value = b'0.0'
            if not allow_leading_whitespace and value[:1].isspace():
                raise ValueError
            if value[-1:].isspace():
                raise ValueError
            out = float(value)
            if math.isnan(out):
                raise ValueError
            if not allow_erange:
                # Values that over- or underflow- are explicitly rejected by
                # redis. This is a crude hack to determine whether the input
                # may have been such a value.
                if out in (math.inf, -math.inf, 0.0) and re.match(b'^[^a-zA-Z]*[1-9]', value):
                    raise ValueError
            return out
        except ValueError:
            raise SimpleError(cls.DECODE_ERROR)

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


class SortFloat(Float):
    DECODE_ERROR = INVALID_SORT_FLOAT_MSG

    @classmethod
    def decode(cls, value):
        return super().decode(
            value, allow_leading_whitespace=True, allow_empty=True, crop_null=True)


class ScoreTest:
    """Argument converter for sorted set score endpoints."""
    def __init__(self, value, exclusive=False):
        self.value = value
        self.exclusive = exclusive

    @classmethod
    def decode(cls, value):
        try:
            exclusive = False
            if value[:1] == b'(':
                exclusive = True
                value = value[1:]
            value = Float.decode(
                value, allow_leading_whitespace=True, allow_erange=True,
                allow_empty=True, crop_null=True)
            return cls(value, exclusive)
        except SimpleError:
            raise SimpleError(INVALID_MIN_MAX_FLOAT_MSG)

    def __str__(self):
        if self.exclusive:
            return '({!r}'.format(self.value)
        else:
            return repr(self.value)

    @property
    def lower_bound(self):
        return (self.value, AfterAny() if self.exclusive else BeforeAny())

    @property
    def upper_bound(self):
        return (self.value, BeforeAny() if self.exclusive else AfterAny())


class StringTest:
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
            raise SimpleError(INVALID_MIN_MAX_STR_MSG)


@functools.total_ordering
class BeforeAny:
    def __gt__(self, other):
        return False

    def __eq__(self, other):
        return isinstance(other, BeforeAny)


@functools.total_ordering
class AfterAny:
    def __lt__(self, other):
        return False

    def __eq__(self, other):
        return isinstance(other, AfterAny)


class Key:
    """Marker to indicate that argument in signature is a key"""
    UNSPECIFIED = object()

    def __init__(self, type_=None, missing_return=UNSPECIFIED):
        self.type_ = type_
        self.missing_return = missing_return


class Signature:
    def __init__(self, name, fixed, repeat=(), flags=""):
        self.name = name
        self.fixed = fixed
        self.repeat = repeat
        self.flags = flags

    def check_arity(self, args):
        if len(args) != len(self.fixed):
            delta = len(args) - len(self.fixed)
            if delta < 0 or not self.repeat:
                raise SimpleError(WRONG_ARGS_MSG.format(self.name))

    def apply(self, args, db):
        """Returns a tuple, which is either:
        - transformed args and a dict of CommandItems; or
        - a single containing a short-circuit return value
        """
        self.check_arity(args)
        if self.repeat:
            delta = len(args) - len(self.fixed)
            if delta % len(self.repeat) != 0:
                raise SimpleError(WRONG_ARGS_MSG.format(self.name))

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
        command_items = []
        for i, (arg, type_) in enumerate(zip(args, types)):
            if isinstance(type_, Key):
                item = db.get(arg)
                default = None
                if type_.type_ is not None:
                    if item is not None and type(item.value) != type_.type_:
                        raise SimpleError(WRONGTYPE_MSG)
                    if item is None:
                        if type_.type_ is not bytes:
                            default = type_.type_()
                args[i] = CommandItem(arg, db, item, default=default)
                command_items.append(args[i])

        return args, command_items


def valid_response_type(value, nested=False):
    if isinstance(value, NoResponse) and not nested:
        return True
    if value is not None and not isinstance(value, (bytes, SimpleString, SimpleError,
                                                    int, list)):
        return False
    if isinstance(value, list):
        if any(not valid_response_type(item, True) for item in value):
            return False
    return True


def command(*args, **kwargs):
    def decorator(func):
        name = kwargs.pop('name', func.__name__)
        func._fakeredis_sig = Signature(name, *args, **kwargs)
        return func

    return decorator


class FakeServer:
    def __init__(self):
        self.lock = threading.Lock()
        self.dbs = defaultdict(lambda: Database(self.lock))
        # Maps SHA1 to script source
        self.script_cache = {}
        # Maps channel/pattern to weak set of sockets
        self.subscribers = defaultdict(weakref.WeakSet)
        self.psubscribers = defaultdict(weakref.WeakSet)
        self.lastsave = int(time.time())
        self.connected = True
        # List of weakrefs to sockets that are being closed lazily
        self.closed_sockets = []


class FakeSocket:
    _connection_error_class = redis.ConnectionError

    def __init__(self, server):
        self._server = server
        self._db = server.dbs[0]
        self._db_num = 0
        # When in a MULTI, set to a list of function calls
        self._transaction = None
        self._transaction_failed = False
        # Set when executing the commands from EXEC
        self._in_transaction = False
        self._watch_notified = False
        self._watches = set()
        self._pubsub = 0      # Count of subscriptions
        self.responses = queue.Queue()
        # Prevents parser from processing commands. Not used in this module,
        # but set by aioredis module to prevent new commands being processed
        # while handling a blocking command.
        self._paused = False
        self._parser = self._parse_commands()
        self._parser.send(None)

    def put_response(self, msg):
        # redis.Connection.__del__ might call self.close at any time, which
        # will set self.responses to None. We assume this will happen
        # atomically, and the code below then protects us against this.
        responses = self.responses
        if responses:
            responses.put(msg)

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False
        self._parser.send(b'')

    def shutdown(self, flags):
        self._parser.close()

    def fileno(self):
        # Our fake socket must return an integer from `FakeSocket.fileno()` since a real selector
        # will be created. The value does not matter since we replace the selector with our own
        # `FakeSelector` before it is ever used.
        return 0

    def _cleanup(self, server):
        """Remove all the references to `self` from `server`.

        This is called with the server lock held, but it may be some time after
        self.close.
        """
        for subs in server.subscribers.values():
            subs.discard(self)
        for subs in server.psubscribers.values():
            subs.discard(self)
        self._clear_watches()

    def close(self):
        # Mark ourselves for cleanup. This might be called from
        # redis.Connection.__del__, which the garbage collection could call
        # at any time, and hence we can't safely take the server lock.
        # We rely on list.append being atomic.
        self._server.closed_sockets.append(weakref.ref(self))
        self._server = None
        self._db = None
        self.responses = None

    @staticmethod
    def _extract_line(buf):
        pos = buf.find(b'\n') + 1
        assert pos > 0
        line = buf[:pos]
        buf = buf[pos:]
        assert line.endswith(b'\r\n')
        return line, buf

    def _parse_commands(self):
        """Generator that parses commands.

        It is fed pieces of redis protocol data (via `send`) and calls
        `_process_command` whenever it has a complete one.
        """
        buf = b''
        while True:
            while self._paused or b'\n' not in buf:
                buf += yield
            line, buf = self._extract_line(buf)
            assert line[:1] == b'*'      # array
            n_fields = int(line[1:-2])
            fields = []
            for i in range(n_fields):
                while b'\n' not in buf:
                    buf += yield
                line, buf = self._extract_line(buf)
                assert line[:1] == b'$'    # string
                length = int(line[1:-2])
                while len(buf) < length + 2:
                    buf += yield
                fields.append(buf[:length])
                buf = buf[length+2:]       # +2 to skip the CRLF
            self._process_command(fields)

    def _run_command(self, func, sig, args, from_script):
        command_items = {}
        try:
            ret = sig.apply(args, self._db)
            if len(ret) == 1:
                result = ret[0]
            else:
                args, command_items = ret
                if from_script and FLAG_NO_SCRIPT in sig.flags:
                    raise SimpleError(COMMAND_IN_SCRIPT_MSG)
                if self._pubsub and sig.name not in [
                        'ping', 'subscribe', 'unsubscribe',
                        'psubscribe', 'punsubscribe', 'quit']:
                    raise SimpleError(BAD_COMMAND_IN_PUBSUB_MSG)
                result = func(*args)
                assert valid_response_type(result)
        except SimpleError as exc:
            result = exc
        for command_item in command_items:
            command_item.writeback()
        return result

    def _decode_error(self, error):
        return redis.connection.BaseParser().parse_error(error.value)

    def _decode_result(self, result):
        """Convert SimpleString and SimpleError, recursively"""
        if isinstance(result, list):
            return [self._decode_result(r) for r in result]
        elif isinstance(result, SimpleString):
            return result.value
        elif isinstance(result, SimpleError):
            return self._decode_error(result)
        else:
            return result

    def _blocking(self, timeout, func):
        """Run a function until it succeeds or timeout is reached.

        The timeout must be an integer, and 0 means infinite. The function
        is called with a boolean to indicate whether this is the first call.
        If it returns None it is considered to have "failed" and is retried
        each time the condition variable is notified, until the timeout is
        reached.

        Returns the function return value, or None if the timeout was reached.
        """
        ret = func(True)
        if ret is not None or self._in_transaction:
            return ret
        if timeout:
            deadline = time.time() + timeout
        else:
            deadline = None
        while True:
            timeout = deadline - time.time() if deadline is not None else None
            if timeout is not None and timeout <= 0:
                return None
            # Python <3.2 doesn't return a status from wait. On Python 3.2+
            # we bail out early on False.
            if self._db.condition.wait(timeout=timeout) is False:
                return None     # Timeout expired
            ret = func(False)
            if ret is not None:
                return ret

    def _name_to_func(self, name):
        name = six.ensure_str(name, encoding='utf-8', errors='replace')
        func_name = name.lower()
        func = getattr(self, func_name, None)
        if name.startswith('_') or not func or not hasattr(func, '_fakeredis_sig'):
            # redis remaps \r or \n in an error to ' ' to make it legal protocol
            clean_name = name.replace('\r', ' ').replace('\n', ' ')
            raise SimpleError(UNKNOWN_COMMAND_MSG.format(clean_name))
        return func, func_name

    def sendall(self, data):
        if not self._server.connected:
            raise self._connection_error_class(CONNECTION_ERROR_MSG)
        if isinstance(data, str):
            data = data.encode('ascii')
        self._parser.send(data)

    def _process_command(self, fields):
        if not fields:
            return
        func_name = None
        try:
            func, func_name = self._name_to_func(fields[0])
            sig = func._fakeredis_sig
            with self._server.lock:
                # Clean out old connections
                while True:
                    try:
                        weak_sock = self._server.closed_sockets.pop()
                    except IndexError:
                        break
                    else:
                        sock = weak_sock()
                        if sock:
                            sock._cleanup(self._server)
                now = time.time()
                for db in self._server.dbs.values():
                    db.time = now
                sig.check_arity(fields[1:])
                # TODO: make a signature attribute for transactions
                if self._transaction is not None \
                        and func_name not in ('exec', 'discard', 'multi', 'watch'):
                    self._transaction.append((func, sig, fields[1:]))
                    result = QUEUED
                else:
                    result = self._run_command(func, sig, fields[1:], False)
        except SimpleError as exc:
            if self._transaction is not None:
                # TODO: should not apply if the exception is from _run_command
                # e.g. watch inside multi
                self._transaction_failed = True
            if func_name == 'exec' and exc.value.startswith('ERR '):
                exc.value = 'EXECABORT Transaction discarded because of: ' + exc.value[4:]
                self._transaction = None
                self._transaction_failed = False
                self._clear_watches()
            result = exc
        result = self._decode_result(result)
        if not isinstance(result, NoResponse):
            self.put_response(result)

    def notify_watch(self):
        self._watch_notified = True

    # redis has inconsistent handling of negative indices, hence two versions
    # of this code.

    @staticmethod
    def _fix_range_string(start, end, length):
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
    def _fix_range(start, end, length):
        # Redis handles negative slightly differently for zrange
        if start < 0:
            start = max(0, start + length)
        if end < 0:
            end += length
        if start > end or start >= length:
            return -1, -1
        end = min(end, length - 1)
        return start, end + 1

    def _scan(self, keys, cursor, *args):
        """
        This is the basis of most of the ``scan`` methods.

        This implementation is KNOWN to be un-performant, as it requires
        grabbing the full set of keys over which we are investigating subsets.

        It also doesn't adhere to the guarantee that every key will be iterated
        at least once even if the database is modified during the scan.
        However, provided the database is not modified, every key will be
        returned exactly once.
        """
        pattern = None
        type = None
        count = 10
        if len(args) % 2 != 0:
            raise SimpleError(SYNTAX_ERROR_MSG)
        for i in range(0, len(args), 2):
            if casematch(args[i], b'match'):
                pattern = args[i + 1]
            elif casematch(args[i], b'count'):
                count = Int.decode(args[i + 1])
                if count <= 0:
                    raise SimpleError(SYNTAX_ERROR_MSG)
            elif casematch(args[i], b'type'):
                type = args[i + 1]
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)

        if cursor >= len(keys):
            return [0, []]
        data = sorted(keys)
        result_cursor = cursor + count
        result_data = []

        regex = compile_pattern(pattern) if pattern is not None else None

        def match_key(key):
            return regex.match(key) if pattern is not None else True

        def match_type(key):
            if type is not None:
                return casematch(self.type(self._db[key]).value, type)
            return True

        if pattern is not None or type is not None:
            for val in itertools.islice(data, cursor, result_cursor):
                compare_val = val[0] if isinstance(val, tuple) else val
                if match_key(compare_val) and match_type(compare_val):
                    result_data.append(val)
        else:
            result_data = data[cursor:result_cursor]

        if result_cursor >= len(data):
            result_cursor = 0
        return [result_cursor, result_data]

    # Connection commands
    # TODO: auth, quit

    @command((bytes,))
    def echo(self, message):
        return message

    @command((), (bytes,))
    def ping(self, *args):
        if len(args) > 1:
            raise SimpleError(WRONG_ARGS_MSG.format('ping'))
        if self._pubsub:
            return [b'pong', args[0] if args else b'']
        else:
            return args[0] if args else PONG

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

    def _delete(self, *keys):
        ans = 0
        done = set()
        for key in keys:
            if key and key.key not in done:
                key.value = None
                done.add(key.key)
                ans += 1
        return ans

    @command((Key(),), (Key(),), name='del')
    def del_(self, *keys):
        return self._delete(*keys)

    @command((Key(),), (Key(),), name='unlink')
    def unlink(self, *keys):
        return self._delete(*keys)

    @command((Key(),), (Key(),))
    def exists(self, *keys):
        ret = 0
        for key in keys:
            if key:
                ret += 1
        return ret

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
    def type(self, key):
        if key.value is None:
            return SimpleString(b'none')
        elif isinstance(key.value, bytes):
            return SimpleString(b'string')
        elif isinstance(key.value, list):
            return SimpleString(b'list')
        elif isinstance(key.value, set):
            return SimpleString(b'set')
        elif isinstance(key.value, ZSet):
            return SimpleString(b'zset')
        elif isinstance(key.value, dict):
            return SimpleString(b'hash')
        else:
            assert False      # pragma: nocover

    @command((Key(),))
    def persist(self, key):
        if key.expireat is None:
            return 0
        key.expireat = None
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
            raise SimpleError(SRC_DST_SAME_MSG)
        if not key or key.key in self._server.dbs[db]:
            return 0
        # TODO: what is the interaction with expiry?
        self._server.dbs[db][key.key] = self._server.dbs[self._db_num][key.key]
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
            raise SimpleError(NO_KEY_MSG)
        # TODO: check interaction with WATCH
        if newkey.key != key.key:
            newkey.value = key.value
            newkey.expireat = key.expireat
            key.value = None
        return OK

    @command((Key(), Key()))
    def renamenx(self, key, newkey):
        if not key:
            raise SimpleError(NO_KEY_MSG)
        if newkey:
            return 0
        self.rename(key, newkey)
        return 1

    @command((Int,), (bytes, bytes))
    def scan(self, cursor, *args):
        return self._scan(list(self._db), cursor, *args)

    def _lookup_key(self, key, pattern):
        """Python implementation of lookupKeyByPattern from redis"""
        if pattern == b'#':
            return key
        p = pattern.find(b'*')
        if p == -1:
            return None
        prefix = pattern[:p]
        suffix = pattern[p+1:]
        arrow = suffix.find(b'->', 0, -1)
        if arrow != -1:
            field = suffix[arrow+2:]
            suffix = suffix[:arrow]
        else:
            field = None
        new_key = prefix + key + suffix
        item = CommandItem(new_key, self._db, item=self._db.get(new_key))
        if item.value is None:
            return None
        if field is not None:
            if not isinstance(item.value, dict):
                return None
            return item.value.get(field)
        else:
            if not isinstance(item.value, bytes):
                return None
            return item.value

    @command((Key(),), (bytes,))
    def sort(self, key, *args):
        i = 0
        desc = False
        alpha = False
        limit_start = 0
        limit_count = -1
        store = None
        sortby = None
        dontsort = False
        get = []
        if key.value is not None:
            if not isinstance(key.value, (set, list, ZSet)):
                raise SimpleError(WRONGTYPE_MSG)

        while i < len(args):
            arg = args[i]
            if casematch(arg, b'asc'):
                desc = False
            elif casematch(arg, b'desc'):
                desc = True
            elif casematch(arg, b'alpha'):
                alpha = True
            elif casematch(arg, b'limit') and i + 2 < len(args):
                try:
                    limit_start = Int.decode(args[i + 1])
                    limit_count = Int.decode(args[i + 2])
                except SimpleError:
                    raise SimpleError(SYNTAX_ERROR_MSG)
                else:
                    i += 2
            elif casematch(arg, b'store') and i + 1 < len(args):
                store = args[i + 1]
                i += 1
            elif casematch(arg, b'by') and i + 1 < len(args):
                sortby = args[i + 1]
                if b'*' not in sortby:
                    dontsort = True
                i += 1
            elif casematch(arg, b'get') and i + 1 < len(args):
                get.append(args[i + 1])
                i += 1
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)
            i += 1

        # TODO: force sorting if the object is a set and either in Lua or
        # storing to a key, to match redis behaviour.
        items = list(key.value) if key.value is not None else []

        # These transformations are based on the redis implementation, but
        # changed to produce a half-open range.
        start = max(limit_start, 0)
        end = len(items) if limit_count < 0 else start + limit_count
        if start >= len(items):
            start = end = len(items) - 1
        end = min(end, len(items))

        if not get:
            get.append(b'#')
        if sortby is None:
            sortby = b'#'

        if not dontsort:
            if alpha:
                def sort_key(v):
                    byval = self._lookup_key(v, sortby)
                    # TODO: use locale.strxfrm when not storing? But then need
                    # to decode too.
                    if byval is None:
                        byval = BeforeAny()
                    return byval

            else:
                def sort_key(v):
                    byval = self._lookup_key(v, sortby)
                    score = SortFloat.decode(byval) if byval is not None else 0.0
                    return (score, v)

            items.sort(key=sort_key, reverse=desc)
        elif isinstance(key.value, (list, ZSet)):
            items.reverse()

        out = []
        for row in items[start:end]:
            for g in get:
                v = self._lookup_key(row, g)
                if store is not None and v is None:
                    v = b''
                out.append(v)
        if store is not None:
            item = CommandItem(store, self._db, item=self._db.get(store))
            item.value = out
            item.writeback()
            return len(out)
        else:
            return out

    @command((Key(missing_return=None),))
    def dump(self, key):
        value = pickle.dumps(key.value)
        checksum = hashlib.sha1(value).digest()
        return checksum + value

    @command((Key(), Int, bytes), (bytes,))
    def restore(self, key, ttl, value, *args):
        replace = False
        i = 0
        while i < len(args):
            if casematch(args[i], b'replace'):
                replace = True
                i += 1
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)
        if key and not replace:
            raise SimpleError(RESTORE_KEY_EXISTS)
        checksum, value = value[:20], value[20:]
        if hashlib.sha1(value).digest() != checksum:
            raise SimpleError(RESTORE_INVALID_CHECKSUM_MSG)
        if ttl < 0:
            raise SimpleError(RESTORE_INVALID_TTL_MSG)
        if ttl == 0:
            expireat = None
        else:
            expireat = self._db.time + ttl / 1000.0
        key.value = pickle.loads(value)
        key.expireat = expireat
        return OK

    # Transaction commands

    def _clear_watches(self):
        self._watch_notified = False
        while self._watches:
            (key, db) = self._watches.pop()
            db.remove_watch(key, self)

    @command((), flags='s')
    def multi(self):
        if self._transaction is not None:
            raise SimpleError(MULTI_NESTED_MSG)
        self._transaction = []
        self._transaction_failed = False
        return OK

    @command((), flags='s')
    def discard(self):
        if self._transaction is None:
            raise SimpleError(WITHOUT_MULTI_MSG.format('DISCARD'))
        self._transaction = None
        self._transaction_failed = False
        self._clear_watches()
        return OK

    @command((), name='exec', flags='s')
    def exec_(self):
        if self._transaction is None:
            raise SimpleError(WITHOUT_MULTI_MSG.format('EXEC'))
        if self._transaction_failed:
            self._transaction = None
            self._clear_watches()
            raise SimpleError(EXECABORT_MSG)
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
                self._in_transaction = True
                ans = self._run_command(func, sig, args, False)
            except SimpleError as exc:
                ans = exc
            finally:
                self._in_transaction = False
            result.append(ans)
        return result

    @command((Key(),), (Key(),), flags='s')
    def watch(self, *keys):
        if self._transaction is not None:
            raise SimpleError(WATCH_INSIDE_MULTI_MSG)
        for key in keys:
            if key not in self._watches:
                self._watches.add((key.key, self._db))
                self._db.add_watch(key.key, self)
        return OK

    @command((), flags='s')
    def unwatch(self):
        self._clear_watches()
        return OK

    # String commands
    # TODO: bitfield, bitop, bitpos

    @command((Key(bytes), bytes))
    def append(self, key, value):
        old = key.get(b'')
        if len(old) + len(value) > MAX_STRING_SIZE:
            raise SimpleError(STRING_OVERFLOW_MSG)
        key.update(key.get(b'') + value)
        return len(key.value)

    @command((Key(bytes, 0),), (bytes,))
    def bitcount(self, key, *args):
        # Redis checks the argument count before decoding integers. That's why
        # we can't declare them as Int.
        if args:
            if len(args) != 2:
                raise SimpleError(SYNTAX_ERROR_MSG)
            start = Int.decode(args[0])
            end = Int.decode(args[1])
            start, end = self._fix_range_string(start, end, len(key.value))
            value = key.value[start:end]
        else:
            value = key.value
        return bin(int.from_bytes(value, 'little')).count('1')

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
            raise SimpleError(NONFINITE_MSG)
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
            actual_val = value[byte]
        except IndexError:
            return 0
        return 1 if (1 << actual_bitoffset) & actual_val else 0

    @command((Key(bytes), BitOffset, BitValue))
    def setbit(self, key, offset, value):
        val = key.get(b'\x00')
        byte = offset // 8
        remaining = offset % 8
        actual_bitoffset = 7 - remaining
        if len(val) - 1 < byte:
            # We need to expand val so that we can set the appropriate
            # bit.
            needed = byte - (len(val) - 1)
            val += b'\x00' * needed
        old_byte = val[byte]
        if value == 1:
            new_byte = old_byte | (1 << actual_bitoffset)
        else:
            new_byte = old_byte & ~(1 << actual_bitoffset)
        old_value = value if old_byte == new_byte else 1 - value
        reconstructed = bytearray(val)
        reconstructed[byte] = new_byte
        key.update(bytes(reconstructed))
        return old_value

    @command((Key(bytes), Int, Int))
    def getrange(self, key, start, end):
        value = key.get(b'')
        start, end = self._fix_range_string(start, end, len(value))
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

    @command((Key(), bytes), (bytes,), name='set')
    def set_(self, key, value, *args):
        i = 0
        ex = None
        px = None
        xx = False
        nx = False
        keepttl = False
        get = False
        while i < len(args):
            if casematch(args[i], b'nx'):
                nx = True
                i += 1
            elif casematch(args[i], b'xx'):
                xx = True
                i += 1
            elif casematch(args[i], b'ex') and i + 1 < len(args):
                ex = Int.decode(args[i + 1])
                if ex <= 0 or (self._db.time + ex) * 1000 >= 2**63:
                    raise SimpleError(INVALID_EXPIRE_MSG.format('set'))
                i += 2
            elif casematch(args[i], b'px') and i + 1 < len(args):
                px = Int.decode(args[i + 1])
                if px <= 0 or self._db.time * 1000 + px >= 2**63:
                    raise SimpleError(INVALID_EXPIRE_MSG.format('set'))
                i += 2
            elif casematch(args[i], b'keepttl'):
                keepttl = True
                i += 1
            elif casematch(args[i], b'get'):
                get = True
                i += 1
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)
        if (xx and nx) or ((px is not None) + (ex is not None) + keepttl > 1):
            raise SimpleError(SYNTAX_ERROR_MSG)
        if nx and get:
            # The command docs say this is allowed from Redis 7.0.
            raise SimpleError(SYNTAX_ERROR_MSG)

        old_value = None
        if get:
            if key.value is not None and type(key.value) is not bytes:
                raise SimpleError(WRONGTYPE_MSG)
            old_value = key.value

        if nx and key:
            return old_value
        if xx and not key:
            return old_value
        if not keepttl:
            key.value = value
        else:
            key.update(value)
        if ex is not None:
            key.expireat = self._db.time + ex
        if px is not None:
            key.expireat = self._db.time + px / 1000.0
        return OK if not get else old_value

    @command((Key(), Int, bytes))
    def setex(self, key, seconds, value):
        if seconds <= 0 or (self._db.time + seconds) * 1000 >= 2**63:
            raise SimpleError(INVALID_EXPIRE_MSG.format('setex'))
        key.value = value
        key.expireat = self._db.time + seconds
        return OK

    @command((Key(), Int, bytes))
    def psetex(self, key, ms, value):
        if ms <= 0 or self._db.time * 1000 + ms >= 2**63:
            raise SimpleError(INVALID_EXPIRE_MSG.format('psetex'))
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
            raise SimpleError(INVALID_OFFSET_MSG)
        elif not value:
            return len(key.get(b''))
        elif offset + len(value) > MAX_STRING_SIZE:
            raise SimpleError(STRING_OVERFLOW_MSG)
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
    def hexists(self, key, field):
        return int(field in key.value)

    @command((Key(Hash), bytes))
    def hget(self, key, field):
        return key.value.get(field)

    @command((Key(Hash),))
    def hgetall(self, key):
        return list(itertools.chain(*key.value.items()))

    @command((Key(Hash), bytes, Int))
    def hincrby(self, key, field, amount):
        c = Int.decode(key.value.get(field, b'0')) + amount
        key.value[field] = Int.encode(c)
        key.updated()
        return c

    @command((Key(Hash), bytes, bytes))
    def hincrbyfloat(self, key, field, amount):
        c = Float.decode(key.value.get(field, b'0')) + Float.decode(amount)
        if not math.isfinite(c):
            raise SimpleError(NONFINITE_MSG)
        encoded = Float.encode(c, True)
        key.value[field] = encoded
        key.updated()
        return encoded

    @command((Key(Hash),))
    def hkeys(self, key):
        return list(key.value.keys())

    @command((Key(Hash),))
    def hlen(self, key):
        return len(key.value)

    @command((Key(Hash), bytes), (bytes,))
    def hmget(self, key, *fields):
        return [key.value.get(field) for field in fields]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hmset(self, key, *args):
        self.hset(key, *args)
        return OK

    @command((Key(Hash), Int,), (bytes, bytes))
    def hscan(self, key, cursor, *args):
        cursor, keys = self._scan(key.value, cursor, *args)
        items = []
        for k in keys:
            items.append(k)
            items.append(key.value[k])
        return [cursor, items]

    @command((Key(Hash), bytes, bytes), (bytes, bytes))
    def hset(self, key, *args):
        h = key.value
        created = 0
        for i in range(0, len(args), 2):
            if args[i] not in h:
                created += 1
            h[args[i]] = args[i + 1]
        key.updated()
        return created

    @command((Key(Hash), bytes, bytes))
    def hsetnx(self, key, field, value):
        if field in key.value:
            return 0
        return self.hset(key, field, value)

    @command((Key(Hash), bytes))
    def hstrlen(self, key, field):
        return len(key.value.get(field, b''))

    @command((Key(Hash),))
    def hvals(self, key):
        return list(key.value.values())

    # List commands

    def _bpop_pass(self, keys, op, first_pass):
        for key in keys:
            item = CommandItem(key, self._db, item=self._db.get(key), default=[])
            if not isinstance(item.value, list):
                if first_pass:
                    raise SimpleError(WRONGTYPE_MSG)
                else:
                    continue
            if item.value:
                ret = op(item.value)
                item.updated()
                item.writeback()
                return [key, ret]
        return None

    def _bpop(self, args, op):
        keys = args[:-1]
        timeout = Timeout.decode(args[-1])
        return self._blocking(timeout, functools.partial(self._bpop_pass, keys, op))

    @command((bytes, bytes), (bytes,), flags='s')
    def blpop(self, *args):
        return self._bpop(args, lambda lst: lst.pop(0))

    @command((bytes, bytes), (bytes,), flags='s')
    def brpop(self, *args):
        return self._bpop(args, lambda lst: lst.pop())

    def _brpoplpush_pass(self, source, destination, first_pass):
        src = CommandItem(source, self._db, item=self._db.get(source), default=[])
        if not isinstance(src.value, list):
            if first_pass:
                raise SimpleError(WRONGTYPE_MSG)
            else:
                return None
        if not src.value:
            return None    # Empty list
        dst = CommandItem(destination, self._db, item=self._db.get(destination), default=[])
        if not isinstance(dst.value, list):
            raise SimpleError(WRONGTYPE_MSG)
        el = src.value.pop()
        dst.value.insert(0, el)
        src.updated()
        src.writeback()
        if destination != source:
            # Ensure writeback only happens once
            dst.updated()
            dst.writeback()
        return el

    @command((bytes, bytes, Timeout), flags='s')
    def brpoplpush(self, source, destination, timeout):
        return self._blocking(timeout,
                              functools.partial(self._brpoplpush_pass, source, destination))

    @command((Key(list, None), Int))
    def lindex(self, key, index):
        try:
            return key.value[index]
        except IndexError:
            return None

    @command((Key(list), bytes, bytes, bytes))
    def linsert(self, key, where, pivot, value):
        if not casematch(where, b'before') and not casematch(where, b'after'):
            raise SimpleError(SYNTAX_ERROR_MSG)
        if not key:
            return 0
        else:
            try:
                index = key.value.index(pivot)
            except ValueError:
                return -1
            if casematch(where, b'after'):
                index += 1
            key.value.insert(index, value)
            key.updated()
            return len(key.value)

    @command((Key(list),))
    def llen(self, key):
        return len(key.value)

    def _list_pop(self, get_slice, key, *args):
        """Implements lpop and rpop.

        `get_slice` must take a count and return a slice expression for the
        range to pop.
        """
        # This implementation is somewhat contorted to match the odd
        # behaviours described in https://github.com/redis/redis/issues/9680.
        count = 1
        if len(args) > 1:
            raise SimpleError(SYNTAX_ERROR_MSG)
        elif len(args) == 1:
            count = args[0]
            if count < 0:
                raise SimpleError(INDEX_ERROR_MSG)
            elif count == 0:
                return None
        if not key:
            return None
        elif type(key.value) != list:
            raise SimpleError(WRONGTYPE_MSG)
        slc = get_slice(count)
        ret = key.value[slc]
        del key.value[slc]
        key.updated()
        if not args:
            ret = ret[0]
        return ret

    @command((Key(),), (Int(),))
    def lpop(self, key, *args):
        return self._list_pop(lambda count: slice(None, count), key, *args)

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
            raise SimpleError(NO_KEY_MSG)
        try:
            key.value[index] = value
            key.updated()
        except IndexError:
            raise SimpleError(INDEX_ERROR_MSG)
        return OK

    @command((Key(list), Int, Int))
    def ltrim(self, key, start, stop):
        if key:
            if stop == -1:
                stop = None
            else:
                stop += 1
            new_value = key.value[start:stop]
            # TODO: check if this should actually be conditional
            if len(new_value) != len(key.value):
                key.update(new_value)
        return OK

    @command((Key(),), (Int(),))
    def rpop(self, key, *args):
        return self._list_pop(lambda count: slice(None, -count - 1, -1), key, *args)

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

    # Set commands

    @command((Key(set), bytes), (bytes,))
    def sadd(self, key, *members):
        old_size = len(key.value)
        key.value.update(members)
        key.updated()
        return len(key.value) - old_size

    @command((Key(set),))
    def scard(self, key):
        return len(key.value)

    def _calc_setop(self, op, stop_if_missing, key, *keys):
        if stop_if_missing and not key.value:
            return set()
        ans = key.value.copy()
        for other in keys:
            value = other.value if other.value is not None else set()
            if not isinstance(value, set):
                raise SimpleError(WRONGTYPE_MSG)
            if stop_if_missing and not value:
                return set()
            ans = op(ans, value)
        return ans

    def _setop(self, op, stop_if_missing, dst, key, *keys):
        """Apply one of SINTER[STORE], SUNION[STORE], SDIFF[STORE].

        If `stop_if_missing`, the output will be made an empty set as soon as
        an empty input set is encountered (use for SINTER[STORE]). May assume
        that `key` is a set (or empty), but `keys` could be anything.
        """
        ans = self._calc_setop(op, stop_if_missing, key, *keys)
        if dst is None:
            return list(ans)
        else:
            dst.value = ans
            return len(dst.value)

    @command((Key(set),), (Key(set),))
    def sdiff(self, *keys):
        return self._setop(lambda a, b: a - b, False, None, *keys)

    @command((Key(), Key(set)), (Key(set),))
    def sdiffstore(self, dst, *keys):
        return self._setop(lambda a, b: a - b, False, dst, *keys)

    @command((Key(set),), (Key(set),))
    def sinter(self, *keys):
        return self._setop(lambda a, b: a & b, True, None, *keys)

    @command((Key(), Key(set)), (Key(set),))
    def sinterstore(self, dst, *keys):
        return self._setop(lambda a, b: a & b, True, dst, *keys)

    @command((Key(set), bytes))
    def sismember(self, key, member):
        return int(member in key.value)

    @command((Key(set),))
    def smembers(self, key):
        return list(key.value)

    @command((Key(set, 0), Key(set), bytes))
    def smove(self, src, dst, member):
        try:
            src.value.remove(member)
            src.updated()
        except KeyError:
            return 0
        else:
            dst.value.add(member)
            dst.updated()   # TODO: is it updated if member was already present?
            return 1

    @command((Key(set),), (Int,))
    def spop(self, key, count=None):
        if count is None:
            if not key.value:
                return None
            item = random.sample(list(key.value), 1)[0]
            key.value.remove(item)
            key.updated()
            return item
        else:
            if count < 0:
                raise SimpleError(INDEX_ERROR_MSG)
            items = self.srandmember(key, count)
            for item in items:
                key.value.remove(item)
                key.updated()   # Inside the loop because redis special-cases count=0
            return items

    @command((Key(set),), (Int,))
    def srandmember(self, key, count=None):
        if count is None:
            if not key.value:
                return None
            else:
                return random.sample(list(key.value), 1)[0]
        elif count >= 0:
            count = min(count, len(key.value))
            return random.sample(list(key.value), count)
        else:
            items = list(key.value)
            return [random.choice(items) for _ in range(-count)]

    @command((Key(set), bytes), (bytes,))
    def srem(self, key, *members):
        old_size = len(key.value)
        for member in members:
            key.value.discard(member)
        deleted = old_size - len(key.value)
        if deleted:
            key.updated()
        return deleted

    @command((Key(set), Int), (bytes, bytes))
    def sscan(self, key, cursor, *args):
        return self._scan(key.value, cursor, *args)

    @command((Key(set),), (Key(set),))
    def sunion(self, *keys):
        return self._setop(lambda a, b: a | b, False, None, *keys)

    @command((Key(), Key(set)), (Key(set),))
    def sunionstore(self, dst, *keys):
        return self._setop(lambda a, b: a | b, False, dst, *keys)

    # Hyperloglog commands
    # These are not quite the same as the real redis ones, which are
    # approximate and store the results in a string. Instead, it is implemented
    # on top of sets.

    @command((Key(set),), (bytes,))
    def pfadd(self, key, *elements):
        result = self.sadd(key, *elements)
        # Per the documentation:
        # - 1 if at least 1 HyperLogLog internal register was altered. 0 otherwise.
        return 1 if result > 0 else 0

    @command((Key(set),), (Key(set),))
    def pfcount(self, *keys):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return len(self.sunion(*keys))

    @command((Key(set), Key(set)), (Key(set),))
    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        self.sunionstore(dest, *sources)
        return OK

    # Sorted set commands
    # TODO: [b]zpopmin/zpopmax,

    @staticmethod
    def _limit_items(items, offset, count):
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

    @staticmethod
    def _apply_withscores(items, withscores):
        if withscores:
            out = []
            for item in items:
                out.append(item[1])
                out.append(Float.encode(item[0], False))
        else:
            out = [item[1] for item in items]
        return out

    @command((Key(ZSet), bytes, bytes), (bytes,))
    def zadd(self, key, *args):
        zset = key.value

        i = 0
        ch = False
        nx = False
        xx = False
        incr = False
        while i < len(args):
            if casematch(args[i], b'ch'):
                ch = True
                i += 1
            elif casematch(args[i], b'nx'):
                nx = True
                i += 1
            elif casematch(args[i], b'xx'):
                xx = True
                i += 1
            elif casematch(args[i], b'incr'):
                incr = True
                i += 1
            else:
                # First argument not matching flags indicates the start of
                # score pairs.
                break

        if nx and xx:
            raise SimpleError(ZADD_NX_XX_ERROR_MSG)

        elements = args[i:]
        if not elements or len(elements) % 2 != 0:
            raise SimpleError(SYNTAX_ERROR_MSG)
        if incr and len(elements) != 2:
            raise SimpleError(ZADD_INCR_LEN_ERROR_MSG)
        # Parse all scores first, before updating
        items = [
            (Float.decode(elements[j]), elements[j + 1])
            for j in range(0, len(elements), 2)
        ]
        old_len = len(zset)
        changed_items = 0

        if incr:
            item_score, item_name = items[0]
            if (nx and item_name in zset) or (xx and item_name not in zset):
                return None
            return self.zincrby(key, item_score, item_name)

        for item_score, item_name in items:
            if (
                (not nx or item_name not in zset)
                and (not xx or item_name in zset)
            ):
                if zset.add(item_name, item_score):
                    changed_items += 1

        if changed_items:
            key.updated()

        if ch:
            return changed_items
        return len(zset) - old_len

    @command((Key(ZSet),))
    def zcard(self, key):
        return len(key.value)

    @command((Key(ZSet), ScoreTest, ScoreTest))
    def zcount(self, key, min, max):
        return key.value.zcount(min.lower_bound, max.upper_bound)

    @command((Key(ZSet), Float, bytes))
    def zincrby(self, key, increment, member):
        # Can't just default the old score to 0.0, because in IEEE754, adding
        # 0.0 to something isn't a nop (e.g. 0.0 + -0.0 == 0.0).
        try:
            score = key.value.get(member, None) + increment
        except TypeError:
            score = increment
        if math.isnan(score):
            raise SimpleError(SCORE_NAN_MSG)
        key.value[member] = score
        key.updated()
        return Float.encode(score, False)

    @command((Key(ZSet), StringTest, StringTest))
    def zlexcount(self, key, min, max):
        return key.value.zlexcount(min.value, min.exclusive, max.value, max.exclusive)

    def _zrange(self, key, start, stop, reverse, *args):
        zset = key.value
        withscores = False
        for arg in args:
            if casematch(arg, b'withscores'):
                withscores = True
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)
        start, stop = self._fix_range(start, stop, len(zset))
        if reverse:
            start, stop = len(zset) - stop, len(zset) - start
        items = zset.islice_score(start, stop, reverse)
        items = self._apply_withscores(items, withscores)
        return items

    @command((Key(ZSet), Int, Int), (bytes,))
    def zrange(self, key, start, stop, *args):
        return self._zrange(key, start, stop, False, *args)

    @command((Key(ZSet), Int, Int), (bytes,))
    def zrevrange(self, key, start, stop, *args):
        return self._zrange(key, start, stop, True, *args)

    def _zrangebylex(self, key, min, max, reverse, *args):
        if args:
            if len(args) != 3 or not casematch(args[0], b'limit'):
                raise SimpleError(SYNTAX_ERROR_MSG)
            offset = Int.decode(args[1])
            count = Int.decode(args[2])
        else:
            offset = 0
            count = -1
        zset = key.value
        items = zset.irange_lex(min.value, max.value,
                                inclusive=(not min.exclusive, not max.exclusive),
                                reverse=reverse)
        items = self._limit_items(items, offset, count)
        return items

    @command((Key(ZSet), StringTest, StringTest), (bytes,))
    def zrangebylex(self, key, min, max, *args):
        return self._zrangebylex(key, min, max, False, *args)

    @command((Key(ZSet), StringTest, StringTest), (bytes,))
    def zrevrangebylex(self, key, max, min, *args):
        return self._zrangebylex(key, min, max, True, *args)

    def _zrangebyscore(self, key, min, max, reverse, *args):
        withscores = False
        offset = 0
        count = -1
        i = 0
        while i < len(args):
            if casematch(args[i], b'withscores'):
                withscores = True
                i += 1
            elif casematch(args[i], b'limit') and i + 2 < len(args):
                offset = Int.decode(args[i + 1])
                count = Int.decode(args[i + 2])
                i += 3
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)
        zset = key.value
        items = list(zset.irange_score(min.lower_bound, max.upper_bound, reverse=reverse))
        items = self._limit_items(items, offset, count)
        items = self._apply_withscores(items, withscores)
        return items

    @command((Key(ZSet), ScoreTest, ScoreTest), (bytes,))
    def zrangebyscore(self, key, min, max, *args):
        return self._zrangebyscore(key, min, max, False, *args)

    @command((Key(ZSet), ScoreTest, ScoreTest), (bytes,))
    def zrevrangebyscore(self, key, max, min, *args):
        return self._zrangebyscore(key, min, max, True, *args)

    @command((Key(ZSet), bytes))
    def zrank(self, key, member):
        try:
            return key.value.rank(member)
        except KeyError:
            return None

    @command((Key(ZSet), bytes))
    def zrevrank(self, key, member):
        try:
            return len(key.value) - 1 - key.value.rank(member)
        except KeyError:
            return None

    @command((Key(ZSet), bytes), (bytes,))
    def zrem(self, key, *members):
        old_size = len(key.value)
        for member in members:
            key.value.discard(member)
        deleted = old_size - len(key.value)
        if deleted:
            key.updated()
        return deleted

    @command((Key(ZSet), StringTest, StringTest))
    def zremrangebylex(self, key, min, max):
        items = key.value.irange_lex(min.value, max.value,
                                     inclusive=(not min.exclusive, not max.exclusive))
        return self.zrem(key, *items)

    @command((Key(ZSet), ScoreTest, ScoreTest))
    def zremrangebyscore(self, key, min, max):
        items = key.value.irange_score(min.lower_bound, max.upper_bound)
        return self.zrem(key, *[item[1] for item in items])

    @command((Key(ZSet), Int, Int))
    def zremrangebyrank(self, key, start, stop):
        zset = key.value
        start, stop = self._fix_range(start, stop, len(zset))
        items = zset.islice_score(start, stop)
        return self.zrem(key, *[item[1] for item in items])

    @command((Key(ZSet), Int), (bytes, bytes))
    def zscan(self, key, cursor, *args):
        new_cursor, ans = self._scan(key.value.items(), cursor, *args)
        flat = []
        for (key, score) in ans:
            flat.append(key)
            flat.append(Float.encode(score, False))
        return [new_cursor, flat]

    @command((Key(ZSet), bytes))
    def zscore(self, key, member):
        try:
            return Float.encode(key.value[member], False)
        except KeyError:
            return None

    @staticmethod
    def _get_zset(value):
        if isinstance(value, set):
            zset = ZSet()
            for item in value:
                zset[item] = 1.0
            return zset
        elif isinstance(value, ZSet):
            return value
        else:
            raise SimpleError(WRONGTYPE_MSG)

    def _zunioninter(self, func, dest, numkeys, *args):
        if numkeys < 1:
            raise SimpleError(ZUNIONSTORE_KEYS_MSG)
        if numkeys > len(args):
            raise SimpleError(SYNTAX_ERROR_MSG)
        aggregate = b'sum'
        sets = []
        for i in range(numkeys):
            item = CommandItem(args[i], self._db, item=self._db.get(args[i]), default=ZSet())
            sets.append(self._get_zset(item.value))
        weights = [1.0] * numkeys

        i = numkeys
        while i < len(args):
            arg = args[i]
            if casematch(arg, b'weights') and i + numkeys < len(args):
                weights = [Float.decode(x) for x in args[i + 1:i + numkeys + 1]]
                i += numkeys + 1
            elif casematch(arg, b'aggregate') and i + 1 < len(args):
                aggregate = casenorm(args[i + 1])
                if aggregate not in (b'sum', b'min', b'max'):
                    raise SimpleError(SYNTAX_ERROR_MSG)
                i += 2
            else:
                raise SimpleError(SYNTAX_ERROR_MSG)

        out_members = set(sets[0])
        for s in sets[1:]:
            if func == 'ZUNIONSTORE':
                out_members |= set(s)
            else:
                out_members.intersection_update(s)

        # We first build a regular dict and turn it into a ZSet. The
        # reason is subtle: a ZSet won't update a score from -0 to +0
        # (or vice versa) through assignment, but a regular dict will.
        out = {}
        # The sort affects the order of floating-point operations.
        # Note that redis uses qsort(1), which has no stability guarantees,
        # so we can't be sure to match it in all cases.
        for s, w in sorted(zip(sets, weights), key=lambda x: len(x[0])):
            for member, score in s.items():
                score *= w
                # Redis only does this step for ZUNIONSTORE. See
                # https://github.com/antirez/redis/issues/3954.
                if func == 'ZUNIONSTORE' and math.isnan(score):
                    score = 0.0
                if member not in out_members:
                    continue
                if member in out:
                    old = out[member]
                    if aggregate == b'sum':
                        score += old
                        if math.isnan(score):
                            score = 0.0
                    elif aggregate == b'max':
                        score = max(old, score)
                    elif aggregate == b'min':
                        score = min(old, score)
                    else:
                        assert False     # pragma: nocover
                if math.isnan(score):
                    score = 0.0
                out[member] = score

        out_zset = ZSet()
        for member, score in out.items():
            out_zset[member] = score

        dest.value = out_zset
        return len(out_zset)

    @command((Key(), Int, bytes), (bytes,))
    def zunionstore(self, dest, numkeys, *args):
        return self._zunioninter('ZUNIONSTORE', dest, numkeys, *args)

    @command((Key(), Int, bytes), (bytes,))
    def zinterstore(self, dest, numkeys, *args):
        return self._zunioninter('ZINTERSTORE', dest, numkeys, *args)

    # Server commands
    # TODO: lots

    @command((), (bytes,), flags='s')
    def bgsave(self, *args):
        if len(args) > 1 or (len(args) == 1 and not casematch(args[0], b'schedule')):
            raise SimpleError(SYNTAX_ERROR_MSG)
        self._server.lastsave = int(time.time())
        return BGSAVE_STARTED

    @command(())
    def dbsize(self):
        return len(self._db)

    @command((), (bytes,))
    def flushdb(self, *args):
        if args:
            if len(args) != 1 or not casematch(args[0], b'async'):
                raise SimpleError(SYNTAX_ERROR_MSG)
        self._db.clear()
        return OK

    @command((), (bytes,))
    def flushall(self, *args):
        if args:
            if len(args) != 1 or not casematch(args[0], b'async'):
                raise SimpleError(SYNTAX_ERROR_MSG)
        for db in self._server.dbs.values():
            db.clear()
        # TODO: clear watches and/or pubsub as well?
        return OK

    @command(())
    def lastsave(self):
        return self._server.lastsave

    @command((), flags='s')
    def save(self):
        self._server.lastsave = int(time.time())
        return OK

    @command(())
    def time(self):
        now_us = round(time.time() * 1000000)
        now_s = now_us // 1000000
        now_us %= 1000000
        return [str(now_s).encode(), str(now_us).encode()]

    # Script commands
    # script debug and script kill will probably not be supported

    def _convert_redis_arg(self, lua_runtime, value):
        # Type checks are exact to avoid issues like bool being a subclass of int.
        if type(value) is bytes:
            return value
        elif type(value) in {int, float}:
            return '{:.17g}'.format(value).encode()
        else:
            # TODO: add the context
            raise SimpleError(LUA_COMMAND_ARG_MSG)

    def _convert_redis_result(self, lua_runtime, result):
        if isinstance(result, (bytes, int)):
            return result
        elif isinstance(result, SimpleString):
            return lua_runtime.table_from({b"ok": result.value})
        elif result is None:
            return False
        elif isinstance(result, list):
            converted = [
                self._convert_redis_result(lua_runtime, item)
                for item in result
            ]
            return lua_runtime.table_from(converted)
        elif isinstance(result, SimpleError):
            raise result
        else:
            raise RuntimeError("Unexpected return type from redis: {}".format(type(result)))

    def _convert_lua_result(self, result, nested=True):
        from lupa import lua_type
        if lua_type(result) == 'table':
            for key in (b'ok', b'err'):
                if key in result:
                    msg = self._convert_lua_result(result[key])
                    if not isinstance(msg, bytes):
                        raise SimpleError(LUA_WRONG_NUMBER_ARGS_MSG)
                    if key == b'ok':
                        return SimpleString(msg)
                    elif nested:
                        return SimpleError(msg.decode('utf-8', 'replace'))
                    else:
                        raise SimpleError(msg.decode('utf-8', 'replace'))
            # Convert Lua tables into lists, starting from index 1, mimicking the behavior of StrictRedis.
            result_list = []
            for index in itertools.count(1):
                if index not in result:
                    break
                item = result[index]
                result_list.append(self._convert_lua_result(item))
            return result_list
        elif isinstance(result, str):
            return result.encode()
        elif isinstance(result, float):
            return int(result)
        elif isinstance(result, bool):
            return 1 if result else None
        return result

    def _check_for_lua_globals(self, lua_runtime, expected_globals):
        actual_globals = set(lua_runtime.globals().keys())
        if actual_globals != expected_globals:
            unexpected = [six.ensure_str(var, 'utf-8', 'replace')
                          for var in actual_globals - expected_globals]
            raise SimpleError(GLOBAL_VARIABLE_MSG.format(", ".join(unexpected)))

    def _lua_redis_call(self, lua_runtime, expected_globals, op, *args):
        # Check if we've set any global variables before making any change.
        self._check_for_lua_globals(lua_runtime, expected_globals)
        func, func_name = self._name_to_func(op)
        args = [self._convert_redis_arg(lua_runtime, arg) for arg in args]
        result = self._run_command(func, func._fakeredis_sig, args, True)
        return self._convert_redis_result(lua_runtime, result)

    def _lua_redis_pcall(self, lua_runtime, expected_globals, op, *args):
        try:
            return self._lua_redis_call(lua_runtime, expected_globals, op, *args)
        except Exception as ex:
            return lua_runtime.table_from({b"err": str(ex)})

    def _lua_redis_log(self, lua_runtime, expected_globals, lvl, *args):
        self._check_for_lua_globals(lua_runtime, expected_globals)
        if len(args) < 1:
            raise SimpleError(REQUIRES_MORE_ARGS_MSG.format("redis.log()", "two"))
        if lvl not in REDIS_LOG_LEVELS.values():
            raise SimpleError(LOG_INVALID_DEBUG_LEVEL_MSG)
        msg = ' '.join([x.decode('utf-8')
                        if isinstance(x, bytes) else str(x)
                        for x in args if not isinstance(x, bool)])
        LOGGER.log(REDIS_LOG_LEVELS_TO_LOGGING[lvl], msg)

    @command((bytes, Int), (bytes,), flags='s')
    def eval(self, script, numkeys, *keys_and_args):
        from lupa import LuaError, LuaRuntime, as_attrgetter

        if numkeys > len(keys_and_args):
            raise SimpleError(TOO_MANY_KEYS_MSG)
        if numkeys < 0:
            raise SimpleError(NEGATIVE_KEYS_MSG)
        sha1 = hashlib.sha1(script).hexdigest().encode()
        self._server.script_cache[sha1] = script
        lua_runtime = LuaRuntime(encoding=None, unpack_returned_tuples=True)

        set_globals = lua_runtime.eval(
            """
            function(keys, argv, redis_call, redis_pcall, redis_log, redis_log_levels)
                redis = {}
                redis.call = redis_call
                redis.pcall = redis_pcall
                redis.log = redis_log
                for level, pylevel in python.iterex(redis_log_levels.items()) do
                    redis[level] = pylevel
                end
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
            functools.partial(self._lua_redis_pcall, lua_runtime, expected_globals),
            functools.partial(self._lua_redis_log, lua_runtime, expected_globals),
            as_attrgetter(REDIS_LOG_LEVELS)
        )
        expected_globals.update(lua_runtime.globals().keys())

        try:
            result = lua_runtime.execute(script)
        except (LuaError, SimpleError) as ex:
            raise SimpleError(SCRIPT_ERROR_MSG.format(sha1.decode(), ex))

        self._check_for_lua_globals(lua_runtime, expected_globals)

        return self._convert_lua_result(result, nested=False)

    @command((bytes, Int), (bytes,), flags='s')
    def evalsha(self, sha1, numkeys, *keys_and_args):
        try:
            script = self._server.script_cache[sha1]
        except KeyError:
            raise SimpleError(NO_MATCHING_SCRIPT_MSG)
        return self.eval(script, numkeys, *keys_and_args)

    @command((bytes,), (bytes,), flags='s')
    def script(self, subcmd, *args):
        if casematch(subcmd, b'load'):
            if len(args) != 1:
                raise SimpleError(BAD_SUBCOMMAND_MSG.format('SCRIPT'))
            script = args[0]
            sha1 = hashlib.sha1(script).hexdigest().encode()
            self._server.script_cache[sha1] = script
            return sha1
        elif casematch(subcmd, b'exists'):
            return [int(sha1 in self._server.script_cache) for sha1 in args]
        elif casematch(subcmd, b'flush'):
            if len(args) > 1 or (len(args) == 1 and casenorm(args[0]) not in {b'sync', b'async'}):
                raise SimpleError(BAD_SUBCOMMAND_MSG.format('SCRIPT'))
            self._server.script_cache = {}
            return OK
        else:
            raise SimpleError(BAD_SUBCOMMAND_MSG.format('SCRIPT'))

    # Pubsub commands
    # TODO: pubsub command

    def _subscribe(self, channels, subscribers, mtype):
        for channel in channels:
            subs = subscribers[channel]
            if self not in subs:
                subs.add(self)
                self._pubsub += 1
            msg = [mtype, channel, self._pubsub]
            self.put_response(msg)
        return NoResponse()

    def _unsubscribe(self, channels, subscribers, mtype):
        if not channels:
            channels = []
            for (channel, subs) in subscribers.items():
                if self in subs:
                    channels.append(channel)
        for channel in channels:
            subs = subscribers.get(channel, set())
            if self in subs:
                subs.remove(self)
                if not subs:
                    del subscribers[channel]
                self._pubsub -= 1
            msg = [mtype, channel, self._pubsub]
            self.put_response(msg)
        return NoResponse()

    @command((bytes,), (bytes,), flags='s')
    def psubscribe(self, *patterns):
        return self._subscribe(patterns, self._server.psubscribers, b'psubscribe')

    @command((bytes,), (bytes,), flags='s')
    def subscribe(self, *channels):
        return self._subscribe(channels, self._server.subscribers, b'subscribe')

    @command((), (bytes,), flags='s')
    def punsubscribe(self, *patterns):
        return self._unsubscribe(patterns, self._server.psubscribers, b'punsubscribe')

    @command((), (bytes,), flags='s')
    def unsubscribe(self, *channels):
        return self._unsubscribe(channels, self._server.subscribers, b'unsubscribe')

    @command((bytes, bytes))
    def publish(self, channel, message):
        receivers = 0
        msg = [b'message', channel, message]
        subs = self._server.subscribers.get(channel, set())
        for sock in subs:
            sock.put_response(msg)
            receivers += 1
        for (pattern, socks) in self._server.psubscribers.items():
            regex = compile_pattern(pattern)
            if regex.match(channel):
                msg = [b'pmessage', pattern, channel, message]
                for sock in socks:
                    sock.put_response(msg)
                    receivers += 1
        return receivers


setattr(FakeSocket, 'del', FakeSocket.del_)
delattr(FakeSocket, 'del_')
setattr(FakeSocket, 'set', FakeSocket.set_)
delattr(FakeSocket, 'set_')
setattr(FakeSocket, 'exec', FakeSocket.exec_)
delattr(FakeSocket, 'exec_')


class _DummyParser:
    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size

    def on_disconnect(self):
        pass

    def on_connect(self, connection):
        pass


# Redis <3.2 will not have a selector
try:
    from redis.selector import BaseSelector
except ImportError:
    class BaseSelector:
        def __init__(self, sock):
            self.sock = sock


class FakeSelector(BaseSelector):
    def check_can_read(self, timeout):
        if self.sock.responses.qsize():
            return True
        if timeout is not None and timeout <= 0:
            return False

        # A sleep/poll loop is easier to mock out than messing with condition
        # variables.
        start = time.time()
        while True:
            if self.sock.responses.qsize():
                return True
            time.sleep(0.01)
            now = time.time()
            if timeout is not None and now > start + timeout:
                return False

    def check_is_ready_for_command(self, timeout):
        return True


class FakeConnection(redis.Connection):
    description_format = "FakeConnection<db=%(db)s>"

    def __init__(self, *args, **kwargs):
        self._server = kwargs.pop('server')
        super().__init__(*args, **kwargs)

    def connect(self):
        super().connect()
        # The selector is set in redis.Connection.connect() after _connect() is called
        self._selector = FakeSelector(self._sock)

    def _connect(self):
        if not self._server.connected:
            raise redis.ConnectionError(CONNECTION_ERROR_MSG)
        return FakeSocket(self._server)

    def can_read(self, timeout=0):
        if not self._server.connected:
            return True
        if not self._sock:
            self.connect()
        # We use check_can_read rather than can_read, because on redis-py<3.2,
        # FakeSelector inherits from a stub BaseSelector which doesn't
        # implement can_read. Normally can_read provides retries on EINTR,
        # but that's not necessary for the implementation of
        # FakeSelector.check_can_read.
        return self._selector.check_can_read(timeout)

    def _decode(self, response):
        if isinstance(response, list):
            return [self._decode(item) for item in response]
        elif isinstance(response, bytes):
            return self.encoder.decode(response)
        else:
            return response

    def read_response(self):
        if not self._server.connected:
            try:
                response = self._sock.responses.get_nowait()
            except queue.Empty:
                raise redis.ConnectionError(CONNECTION_ERROR_MSG)
        else:
            response = self._sock.responses.get()
        if isinstance(response, redis.ResponseError):
            raise response
        return self._decode(response)

    def repr_pieces(self):
        pieces = [
            ('server', self._server),
            ('db', self.db)
        ]
        if self.client_name:
            pieces.append(('client_name', self.client_name))
        return pieces


class FakeRedisMixin:
    def __init__(self, *args, server=None, connected=True, **kwargs):
        # Interpret the positional and keyword arguments according to the
        # version of redis in use.
        bound = _ORIG_SIG.bind(*args, **kwargs)
        bound.apply_defaults()
        if not bound.arguments['connection_pool']:
            charset = bound.arguments['charset']
            errors = bound.arguments['errors']
            # Adapted from redis-py
            if charset is not None:
                warnings.warn(DeprecationWarning(
                    '"charset" is deprecated. Use "encoding" instead'))
                bound.arguments['encoding'] = charset
            if errors is not None:
                warnings.warn(DeprecationWarning(
                    '"errors" is deprecated. Use "encoding_errors" instead'))
                bound.arguments['encoding_errors'] = errors

            if server is None:
                server = FakeServer()
                server.connected = connected
            kwargs = {
                'connection_class': FakeConnection,
                'server': server
            }
            conn_pool_args = [
                'db',
                'username',
                'password',
                'socket_timeout',
                'encoding',
                'encoding_errors',
                'decode_responses',
                'retry_on_timeout',
                'max_connections',
                'health_check_interval',
                'client_name'
            ]
            for arg in conn_pool_args:
                if arg in bound.arguments:
                    kwargs[arg] = bound.arguments[arg]
            bound.arguments['connection_pool'] = redis.connection.ConnectionPool(**kwargs)
        super().__init__(*bound.args, **bound.kwargs)

    @classmethod
    def from_url(cls, *args, **kwargs):
        server = kwargs.pop('server', None)
        if server is None:
            server = FakeServer()
        pool = redis.ConnectionPool.from_url(*args, **kwargs)
        # Now override how it creates connections
        pool.connection_class = FakeConnection
        pool.connection_kwargs['server'] = server
        # FakeConnection cannot handle the path kwarg (present when from_url
        # is called with a unix socket)
        pool.connection_kwargs.pop('path', None)
        return cls(connection_pool=pool)


class FakeStrictRedis(FakeRedisMixin, redis.StrictRedis):
    pass


class FakeRedis(FakeRedisMixin, redis.Redis):
    pass
