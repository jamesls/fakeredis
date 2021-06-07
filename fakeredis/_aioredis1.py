import asyncio
import sys
import warnings

import aioredis

from . import _async, _server


class FakeSocket(_async.AsyncFakeSocket):
    def _decode_error(self, error):
        return aioredis.ReplyError(error.value)


class FakeReader:
    """Re-implementation of aioredis.stream.StreamReader.

    It does not use a socket, but instead provides a queue that feeds
    `readobj`.
    """

    def __init__(self, socket):
        self._socket = socket

    def set_parser(self, parser):
        pass       # No parser needed, we get already-parsed data

    async def readobj(self):
        if self._socket.responses is None:
            raise asyncio.CancelledError
        result = await self._socket.responses.get()
        return result

    def at_eof(self):
        return self._socket.responses is None

    def feed_obj(self, obj):
        self._queue.put_nowait(obj)


class FakeWriter:
    """Replaces a StreamWriter for an aioredis connection."""

    def __init__(self, socket):
        self.transport = socket       # So that aioredis can call writer.transport.close()

    def write(self, data):
        self.transport.sendall(data)


class FakeConnectionsPool(aioredis.ConnectionsPool):
    def __init__(self, server=None, db=None, password=None, encoding=None,
                 *, minsize, maxsize, ssl=None, parser=None,
                 create_connection_timeout=None,
                 connection_cls=None,
                 loop=None):
        super().__init__('fakeredis',
                         db=db,
                         password=password,
                         encoding=encoding,
                         minsize=minsize,
                         maxsize=maxsize,
                         ssl=ssl,
                         parser=parser,
                         create_connection_timeout=create_connection_timeout,
                         connection_cls=connection_cls,
                         loop=loop)
        if server is None:
            server = _server.FakeServer()
        self._server = server

    def _create_new_connection(self, address):
        # TODO: what does address do here? Might just be for sentinel?
        return create_connection(self._server,
                                 db=self._db,
                                 password=self._password,
                                 ssl=self._ssl,
                                 encoding=self._encoding,
                                 parser=self._parser_class,
                                 timeout=self._create_connection_timeout,
                                 connection_cls=self._connection_cls,
                                 )


async def create_connection(server=None, *, db=None, password=None, ssl=None,
                            encoding=None, parser=None, loop=None,
                            timeout=None, connection_cls=None):
    # This is mostly copied from aioredis.connection.create_connection
    if timeout is not None and timeout <= 0:
        raise ValueError("Timeout has to be None or a number greater than 0")

    if connection_cls:
        assert issubclass(connection_cls, aioredis.abc.AbcConnection),\
                "connection_class does not meet the AbcConnection contract"
        cls = connection_cls
    else:
        cls = aioredis.connection.RedisConnection

    if loop is not None and sys.version_info >= (3, 8, 0):
        warnings.warn("The loop argument is deprecated",
                      DeprecationWarning)

    if server is None:
        server = _server.FakeServer()
    socket = FakeSocket(server)
    reader = FakeReader(socket)
    writer = FakeWriter(socket)
    conn = cls(reader, writer, encoding=encoding,
               address='fakeredis', parser=parser)

    try:
        if password is not None:
            await conn.auth(password)
        if db is not None:
            await conn.select(db)
    except Exception:
        conn.close()
        await conn.wait_closed()
        raise
    return conn


async def create_redis(server=None, *, db=None, password=None, ssl=None,
                       encoding=None, commands_factory=aioredis.Redis,
                       parser=None, timeout=None,
                       connection_cls=None, loop=None):
    conn = await create_connection(server, db=db,
                                   password=password,
                                   ssl=ssl,
                                   encoding=encoding,
                                   parser=parser,
                                   timeout=timeout,
                                   connection_cls=connection_cls,
                                   loop=loop)
    return commands_factory(conn)


async def create_pool(server=None, *, db=None, password=None, ssl=None,
                      encoding=None, minsize=1, maxsize=10,
                      parser=None, loop=None, create_connection_timeout=None,
                      pool_cls=None, connection_cls=None):
    # Mostly copied from aioredis.pool.create_pool.
    if pool_cls:
        assert issubclass(pool_cls, aioredis.AbcPool),\
                "pool_class does not meet the AbcPool contract"
        cls = pool_cls
    else:
        cls = FakeConnectionsPool

    pool = cls(server, db, password, encoding,
               minsize=minsize, maxsize=maxsize,
               ssl=ssl, parser=parser,
               create_connection_timeout=create_connection_timeout,
               connection_cls=connection_cls,
               loop=loop)
    try:
        await pool._fill_free(override_min=False)
    except Exception:
        pool.close()
        await pool.wait_closed()
        raise
    return pool


async def create_redis_pool(server=None, *, db=None, password=None, ssl=None,
                            encoding=None, commands_factory=aioredis.Redis,
                            minsize=1, maxsize=10, parser=None,
                            timeout=None, pool_cls=None,
                            connection_cls=None, loop=None):
    pool = await create_pool(server, db=db,
                             password=password,
                             ssl=ssl,
                             encoding=encoding,
                             minsize=minsize,
                             maxsize=maxsize,
                             parser=parser,
                             create_connection_timeout=timeout,
                             pool_cls=pool_cls,
                             connection_cls=connection_cls,
                             loop=loop)
    return commands_factory(pool)
