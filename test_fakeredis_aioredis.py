import asyncio

import asynctest
import pytest
import aioredis

import fakeredis.aioredis


class TestFakeCommands(asynctest.TestCase):
    async def setUp(self):
        self.redis = await fakeredis.aioredis.create_redis_pool()

    async def tearDown(self):
        self.redis.close()
        await self.redis.wait_closed()

    async def test_ping(self):
        pong = await self.redis.ping()
        assert pong == b'PONG'

    async def test_types(self):
        await self.redis.hmset_dict('hash', key1='value1', key2='value2', key3=123)
        result = await self.redis.hgetall('hash', encoding='utf-8')
        assert result == {
            'key1': 'value1',
            'key2': 'value2',
            'key3': '123'
        }

    async def test_transaction(self):
        tr = self.redis.multi_exec()
        tr.set('key1', 'value1')
        tr.set('key2', 'value2')
        ok1, ok2 = await tr.execute()
        assert ok1
        assert ok2
        result = await self.redis.get('key1')
        assert result == b'value1'

    async def test_pubsub(self):
        ch, = await self.redis.subscribe('channel')
        queue = asyncio.Queue()

        async def reader(channel):
            async for message in ch.iter():
                queue.put_nowait(message)

        task = self.loop.create_task(reader(ch))
        await self.redis.publish('channel', 'message1')
        await self.redis.publish('channel', 'message2')
        result1 = await queue.get()
        result2 = await queue.get()
        assert result1 == b'message1'
        assert result2 == b'message2'
        ch.close()
        await task

    async def test_blocking_ready(self):
        self.redis.rpush('list', 'x')
        with await self.redis as r:
            result = await r.blpop('list', timeout=1)
        assert result == [b'list', b'x']

    @pytest.mark.slow
    async def test_blocking_timeout(self):
        with await self.redis as r:
            result = await r.blpop('missing', timeout=1)
        assert result is None

    @pytest.mark.slow
    async def test_blocking_unblock(self):
        async def unblock():
            await asyncio.sleep(0.1)
            await self.redis.rpush('list', 'y')

        with await self.redis as r:
            task = self.loop.create_task(unblock())
            result = await r.blpop('list', timeout=1)
        assert result == [b'list', b'y']
        await task


class TestRealCommands(TestFakeCommands):
    async def setUp(self):
        try:
            self.redis = await aioredis.create_redis_pool('redis://localhost')
            await self.redis.ping()
        except ConnectionRefusedError:
            pytest.skip('redis is not running')
