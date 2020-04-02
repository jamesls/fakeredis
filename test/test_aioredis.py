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

    async def test_transaction_fail(self):
        # ensure that the WATCH applies to the same connection as the MULTI/EXEC.
        await self.redis.set('foo', '1')
        with await self.redis as r:
            await r.watch('foo')
            await self.redis.set('foo', '2')    # Different connection
            tr = r.multi_exec()
            tr.get('foo')
            with pytest.raises(aioredis.MultiExecError):
                await tr.execute()

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
        """Blocking command which does not need to block."""
        self.redis.rpush('list', 'x')
        with await self.redis as r:
            result = await r.blpop('list', timeout=1)
        assert result == [b'list', b'x']

    @pytest.mark.slow
    async def test_blocking_timeout(self):
        """Blocking command that times out without completing."""
        with await self.redis as r:
            result = await r.blpop('missing', timeout=1)
        assert result is None

    @pytest.mark.slow
    async def test_blocking_unblock(self):
        """Blocking command that gets unblocked after some time."""
        async def unblock():
            await asyncio.sleep(0.1)
            await self.redis.rpush('list', 'y')

        with await self.redis as r:
            task = self.loop.create_task(unblock())
            result = await r.blpop('list', timeout=1)
        assert result == [b'list', b'y']
        await task

    @pytest.mark.slow
    async def test_blocking_pipeline(self):
        """Blocking command with another command issued behind it."""
        with await self.redis as r:   # Ensure commands use same connection
            await r.set('foo', 'bar')
            fut = asyncio.ensure_future(r.blpop('list', timeout=1))
            assert (await r.get('foo')) == b'bar'
            assert (await fut) is None

    async def test_wrongtype_error(self):
        await self.redis.set('foo', 'bar')
        with pytest.raises(aioredis.ReplyError) as excinfo:
            await self.redis.rpush('foo', 'baz')
        assert str(excinfo.value).startswith('WRONGTYPE ')

    async def test_syntax_error(self):
        with pytest.raises(aioredis.ReplyError) as excinfo:
            await self.redis.execute('get')
        assert str(excinfo.value) == "ERR wrong number of arguments for 'get' command"

    async def test_no_script_error(self):
        with pytest.raises(aioredis.ReplyError) as excinfo:
            await self.redis.evalsha('0123456789abcdef0123456789abcdef')
        assert str(excinfo.value).startswith('NOSCRIPT ')

    async def test_failed_script_error(self):
        await self.redis.set('foo', 'bar')
        with pytest.raises(aioredis.ReplyError) as excinfo:
            await self.redis.eval(
                'return redis.call("ZCOUNT", KEYS[1])', ['foo'])
        assert str(excinfo.value).startswith('ERR Error running script')


class TestRealCommands(TestFakeCommands):
    async def setUp(self):
        try:
            self.redis = await aioredis.create_redis_pool('redis://localhost')
            await self.redis.flushall()
        except ConnectionRefusedError:
            pytest.skip('redis is not running')

    async def tearDown(self):
        await self.redis.flushall()
        await super().tearDown()
