import asyncio

import async_timeout

from . import _server


class AsyncFakeSocket(_server.FakeSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.responses = asyncio.Queue()

    def put_response(self, msg):
        self.responses.put_nowait(msg)

    async def _async_blocking(self, timeout, func, event, callback):
        try:
            result = None
            with async_timeout.timeout(timeout if timeout else None):
                while True:
                    await event.wait()
                    event.clear()
                    # This is a coroutine outside the normal control flow that
                    # locks the server, so we have to take our own lock.
                    with self._server.lock:
                        ret = func(False)
                        if ret is not None:
                            result = self._decode_result(ret)
                            self.put_response(result)
                            break
        except asyncio.TimeoutError:
            result = None
        finally:
            with self._server.lock:
                self._db.remove_change_callback(callback)
            self.put_response(result)
            self.resume()

    def _blocking(self, timeout, func):
        loop = asyncio.get_event_loop()
        ret = func(True)
        if ret is not None or self._in_transaction:
            return ret
        event = asyncio.Event()

        def callback():
            loop.call_soon_threadsafe(event.set)
        self._db.add_change_callback(callback)
        self.pause()
        loop.create_task(self._async_blocking(timeout, func, event, callback))
        return _server.NoResponse()
