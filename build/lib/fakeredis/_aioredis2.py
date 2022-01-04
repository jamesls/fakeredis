import asyncio
from typing import Union

import aioredis

from . import _async, _server


class FakeSocket(_async.AsyncFakeSocket):
    _connection_error_class = aioredis.ConnectionError

    def _decode_error(self, error):
        return aioredis.connection.BaseParser(1).parse_error(error.value)


class FakeReader:
    pass


class FakeWriter:
    def __init__(self, socket: FakeSocket) -> None:
        self._socket = socket

    def close(self):
        self._socket = None

    async def wait_closed(self):
        pass

    async def drain(self):
        pass

    def writelines(self, data):
        for chunk in data:
            self._socket.sendall(chunk)


class FakeConnection(aioredis.Connection):
    def __init__(self, *args, **kwargs):
        self._server = kwargs.pop('server')
        self._sock = None
        super().__init__(*args, **kwargs)

    async def _connect(self):
        if not self._server.connected:
            raise aioredis.ConnectionError(_server.CONNECTION_ERROR_MSG)
        self._sock = FakeSocket(self._server)
        self._reader = FakeReader()
        self._writer = FakeWriter(self._sock)

    async def disconnect(self):
        await super().disconnect()
        self._sock = None

    async def can_read(self, timeout: float = 0):
        if not self.is_connected:
            await self.connect()
        if timeout == 0:
            return not self._sock.responses.empty()
        # asyncio.Queue doesn't have a way to wait for the queue to be
        # non-empty without consuming an item, so kludge it with a sleep/poll
        # loop.
        loop = asyncio.get_event_loop()
        start = loop.time()
        while True:
            if not self._sock.responses.empty():
                return True
            await asyncio.sleep(0.01)
            now = loop.time()
            if timeout is not None and now > start + timeout:
                return False

    def _decode(self, response):
        if isinstance(response, list):
            return [self._decode(item) for item in response]
        elif isinstance(response, bytes):
            return self.encoder.decode(response)
        else:
            return response

    async def read_response(self):
        if not self._server.connected:
            try:
                response = self._sock.responses.get_nowait()
            except asyncio.QueueEmpty:
                raise aioredis.ConnectionError(_server.CONNECTION_ERROR_MSG)
        else:
            response = await self._sock.responses.get()
        if isinstance(response, aioredis.ResponseError):
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


class FakeRedis(aioredis.Redis):
    def __init__(
        self,
        *,
        db: Union[str, int] = 0,
        password: str = None,
        socket_timeout: float = None,
        connection_pool: aioredis.ConnectionPool = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        max_connections: int = None,
        health_check_interval: int = 0,
        client_name: str = None,
        username: str = None,
        server: _server.FakeServer = None,
        connected: bool = True,
        **kwargs
    ):
        if not connection_pool:
            # Adapted from aioredis
            if server is None:
                server = _server.FakeServer()
                server.connected = connected
            connection_kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "socket_timeout": socket_timeout,
                "encoding": encoding,
                "encoding_errors": encoding_errors,
                "decode_responses": decode_responses,
                "retry_on_timeout": retry_on_timeout,
                "max_connections": max_connections,
                "health_check_interval": health_check_interval,
                "client_name": client_name,
                "server": server,
                "connection_class": FakeConnection
            }
            connection_pool = aioredis.ConnectionPool(**connection_kwargs)
        super().__init__(
            db=db,
            password=password,
            socket_timeout=socket_timeout,
            connection_pool=connection_pool,
            encoding=encoding,
            encoding_errors=encoding_errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            max_connections=max_connections,
            health_check_interval=health_check_interval,
            client_name=client_name,
            username=username,
            **kwargs
        )

    @classmethod
    def from_url(cls, url: str, **kwargs):
        server = kwargs.pop('server', None)
        if server is None:
            server = _server.FakeServer()
        self = super().from_url(url, **kwargs)
        # Now override how it creates connections
        pool = self.connection_pool
        pool.connection_class = FakeConnection
        pool.connection_kwargs['server'] = server
        return self
