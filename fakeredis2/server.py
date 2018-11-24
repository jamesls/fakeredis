import os
import io
import queue
import functools
import time
import threading
import math
import re
from collections import defaultdict
try:
    # Python 3.8+ https://docs.python.org/3/whatsnew/3.7.html#id3
    from collections.abc import MutableMapping
except ImportError:
    # Python 2.6, 2.7
    from collections import MutableMapping

import redis
from redis._compat import nativestr


INVALID_EXPIRE_MSG = "invalid expire time in set"
WRONGTYPE_MSG = \
    "WRONGTYPE Operation against a key holding the wrong kind of value"
SYNTAX_ERROR_MSG = "syntax error"
INVALID_INT_MSG = "value is not an integer or out of range"
INVALID_FLOAT_MSG = "value is not a valid float"
INVALID_BIT_OFFSET_MSG = "bit offset is not an integer or out of range"
INVALID_DB_MSG = "DB index is out of range"
OVERFLOW_MSG = "increment or decrement would overflow"
NONFINITE_MSG = "increment would produce NaN or Infinity"
WRONG_ARGS_MSG = "wrong number of arguments for '{}' command"
UNKNOWN_COMMAND_MSG = "unknown command '{}'"
OK = b'OK'


# TODO: Python 2 support
def byte_to_int(b):
    assert isinstance(b, int)
    return b


class Item(object):
    __slots__ = ['value', 'expireat', 'version']

    def __init__(self, value):
        self.value = value
        self.expireat = None
        self.version = 1

    def get(self, default):
        return self.value if self.value is not None else default

    def replace(self, new_value):
        self.value = new_value
        self.version += 1
        self.expireat = None

    def update(self, new_value):
        self.value = new_value
        self.version += 1

    def updated(self):
        self.version += 1


class ExpiringDict(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self.time = 0.0

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


class ZSet(dict):
    redis_type = b'zset'


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
        return 0 <= value < 8 * 512 * 1024 * 1024     # Redis imposes 512MB limit on keys


class DbIndex(Int):
    """Argument converted for databased indices"""

    DECODE_ERROR = INVALID_DB_MSG

    @classmethod
    def valid(cls, value):
        return 0 <= value < 16


class Float(object):
    """Argument converter for double-precision floats"""

    @classmethod
    def decode(cls, value):
        try:
            return float(value)
        except ValueError:
            raise redis.ResponseError(INVALID_FLOAT_MSG)


class LongDouble(object):
    """Argument converter for long double"""

    @classmethod
    def decode(cls, value):
        import numpy as np
        try:
            out = np.longdouble(value)
            # Checks done by redis
            if value[:1].isspace() or np.isnan(out):
                raise ValueError
            return out
        except ValueError:
            raise redis.ResponseError(INVALID_FLOAT_MSG)

    @classmethod
    def encode(cls, value):
        import numpy as np
        if np.isfinite(value):
            # This mimics how redis does the conversion, even though it is
            # not lossless.
            s = np.format_float_positional(value, precision=17,
                                           unique=False, trim='-')
            # numpy bug prevents decimal point being removed in some cases
            if s.endswith('.'):
                s = s[:-1]
            return s.encode('ascii')
        else:
            raise redis.ResponseError(NONFINITE_MSG)


class Key(object):
    """Marker to indicate that argument in signature is a key"""

    def __init__(self, type_=None):
        self.type_ = type_


class Signature(object):
    def __init__(self, name, fixed, repeat=()):
        self.name = name
        self.fixed = fixed
        self.repeat = repeat

    def apply(self, args, db):
        if len(args) != len(self.fixed):
            delta = len(args) - len(self.fixed)
            if delta < 0 or not self.repeat or delta % len(self.repeat) != 0:
                raise redis.ResponseError(WRONG_ARGS_MSG.format(self.name))

        types = list(self.fixed)
        for i in range(len(args) - len(types)):
            types.append(self.repeat[i % len(self.repeat)])

        args = list(args)
        # First pass: convert/validate non-keys
        for i, (arg, type_) in enumerate(zip(args, types)):
            if not isinstance(type_, Key) and type_ != bytes:
                args[i] = type_.decode(args[i])

        # Second pass: read keys and check their types
        tmp_keys = {}
        for i, (arg, type_) in enumerate(zip(args, types)):
            if isinstance(type_, Key):
                value = db.get(arg, Item(None))
                if type_.type_ is not None:
                    if value.value is not None and type(value.value) != type_.type_:
                        raise redis.ResponseError(WRONGTYPE_MSG)
                    if value.value is None and type_.type_ is not bytes:
                        value.replace(type_.type_())
                tmp_keys[arg] = value
                args[i] = value

        return args, tmp_keys


def command(*args, **kwargs):
    def decorator(func):
        name = kwargs.pop('name', func.__name__)
        func._fakeredis_sig = Signature(name, *args, **kwargs)
        return func

    return decorator


class FakeServer(object):
    def __init__(self):
        self.dbs = defaultdict(ExpiringDict)
        self.lock = threading.Lock()


class FakeSocket(object):
    def __init__(self, server):
        self._server = server
        self._db = server.dbs[0]
        self._db_num = 0
        self.responses = queue.Queue()

    def shutdown(self, flags):
        pass     # For compatibility with socket.socket

    def close(self):
        # TODO: unsubscribe from pub/sub
        self._server = None
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
            func = getattr(self, name.lower(), None)
            if name.startswith('_') or not func or not hasattr(func, '_fakeredis_sig'):
                # redis remaps \r or \n in an error to ' ' to make it legal protocol
                clean_name = name.replace('\r', ' ').replace('\n', ' ')
                raise redis.ResponseError(UNKNOWN_COMMAND_MSG.format(clean_name))
            sig = func._fakeredis_sig
            with self._server.lock:
                now = time.time()
                for db in self._server.dbs.values():
                    db.time = now
                args, tmp_keys = sig.apply(fields[1:], self._db)
                result = func(*args)
                # Remove empty containers, and make temporary items permanent
                for key, item in tmp_keys.items():
                    if isinstance(item.value, bytes) or item.value:
                        self._db[key] = item
                    else:
                        self._db.pop(key, None)
                # TODO: decode results if requested
        except redis.ResponseError as exc:
            result = exc
        self.responses.put(result)

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

    # TODO: implement auth, quit

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
            tmp = dict(db1)
            db1.clear()
            db1.update(db2)
            db2.clear()
            db2.update(tmp)
        return OK

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
        # TODO: clear pubsub as well?
        return OK

    @command((Key(bytes), bytes))
    def append(self, key, value):
        key.update(key.get(b'') + value)
        return len(key.value)

    @command((Key(bytes),), (bytes,))
    def bitcount(self, key, *args):
        # Redis immediately returns 0 if the key doesn't exist, without
        # validating the optional args. That's why we can't declare them as
        # int.
        if key.value is None:
            return 0
        if args:
            if len(args) != 2:
                raise redis.ResponseError(SYNTAX_ERR_MSG)
            start = Int.decode(args[0])
            end = Int.decode(args[1])
            start, end = self._fix_range(start, end, len(key.value))
            value = key.value[start:end]
        else:
            value = key.value
        return sum([bin(byte_to_int(l)).count('1') for l in value])

    @command((Key(bytes), Int))
    def incrby(self, key, amount):
        c = Int.decode(key.get(b'0')) + amount
        key.update(Int.encode(c))
        return c

    @command((Key(bytes), Int))
    def decrby(self, key, amount):
        return self.incrby(key, -amount)

    @command((Key(bytes),))
    def decr(self, key):
        return self.incrby(key, -1)

    @command((Key(bytes),))
    def incr(self, key):
        return self.incrby(key, 1)

    @command((Key(bytes), bytes))
    def incrbyfloat(self, key, amount):
        # TODO: introduce convert_order so that we can specify amount is LongDouble
        c = LongDouble.decode(key.get(b'0')) + LongDouble.decode(amount)
        encoded = LongDouble.encode(c)
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
        key.replace(value)
        return old

    @command((Key(Hash), bytes, bytes))
    def hset(self, key, field, value):
        h = key.value
        is_new = field not in h
        h[field] = value
        key.updated()
        return 1 if is_new else 0

    @command((Key(Hash), bytes))
    def hget(self, key, field):
        return key.value.get(field)

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
            elif args[i].lower() == b'ex' and i + 1 < args.length:
                ex = to_int(args[i + 1])
                if ex <= 0:
                    raise redis.ResponseError(INVALID_EXPIRE_MSG)
                i += 2
            elif args[i].lower() == b'px' and i + 1 < args.length:
                px = to_int(args[i + 1])
                if px <= 0:
                    raise redis.ResponseError(INVALID_EXPIRE_MSG)
                i += 2
            else:
                raise redis.ResponseError(SYNTAX_ERROR_MSG)
        if (xx and nx) or (px is not None and ex is not None):
            raise redis.ResponseError(SYNTAX_ERROR_MSG)

        if nx and key.value is not None:
            return None
        if xx and key.value is None:
            return None
        key.replace(value)
        if ex is not None:
            key.expireat = self._time + ex
        if px is not None:
            key.expireat = self._time + px / 1000.0
        return OK

    @command((Key(),), name='del')
    def delete(self, key):
        key.value = None
        return OK


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
