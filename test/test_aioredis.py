import asyncio

import pytest
import aioredis
from async_generator import yield_, async_generator

import fakeredis.aioredis


pytestmark = [pytest.mark.asyncio]


@pytest.fixture(
    params=[
        pytest.param('fake', marks=pytest.mark.fake),
        pytest.param('real', marks=pytest.mark.real)
    ]
)
@async_generator
async def r(request):
    if request.param == 'fake':
        ret = await fakeredis.aioredis.create_redis_pool()
    else:
        if not request.getfixturevalue('is_redis_running'):
            pytest.skip('Redis is not running')
        ret = await aioredis.create_redis_pool('redis://localhost')
    await ret.flushall()

    await yield_(ret)

    await ret.flushall()
    ret.close()
    await ret.wait_closed()


@pytest.fixture
@async_generator
async def conn(r):
    """A single connection, rather than a pool."""
    with await r as conn:
        await yield_(conn)


async def test_ping(r):
    pong = await r.ping()
    assert pong == b'PONG'


async def test_types(r):
    await r.hmset_dict('hash', key1='value1', key2='value2', key3=123)
    result = await r.hgetall('hash', encoding='utf-8')
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        'key3': '123'
    }


async def test_transaction(r):
    tr = r.multi_exec()
    tr.set('key1', 'value1')
    tr.set('key2', 'value2')
    ok1, ok2 = await tr.execute()
    assert ok1
    assert ok2
    result = await r.get('key1')
    assert result == b'value1'


async def test_transaction_fail(r, conn):
    # ensure that the WATCH applies to the same connection as the MULTI/EXEC.
    await r.set('foo', '1')
    await conn.watch('foo')
    await conn.set('foo', '2')    # Different connection
    tr = conn.multi_exec()
    tr.get('foo')
    with pytest.raises(aioredis.MultiExecError):
        await tr.execute()


async def test_pubsub(r, event_loop):
    ch, = await r.subscribe('channel')
    queue = asyncio.Queue()

    async def reader(channel):
        async for message in ch.iter():
            queue.put_nowait(message)

    task = event_loop.create_task(reader(ch))
    await r.publish('channel', 'message1')
    await r.publish('channel', 'message2')
    result1 = await queue.get()
    result2 = await queue.get()
    assert result1 == b'message1'
    assert result2 == b'message2'
    ch.close()
    await task


async def test_blocking_ready(r, conn):
    """Blocking command which does not need to block."""
    await r.rpush('list', 'x')
    result = await conn.blpop('list', timeout=1)
    assert result == [b'list', b'x']


@pytest.mark.slow
async def test_blocking_timeout(conn):
    """Blocking command that times out without completing."""
    result = await conn.blpop('missing', timeout=1)
    assert result is None


@pytest.mark.slow
async def test_blocking_unblock(r, conn, event_loop):
    """Blocking command that gets unblocked after some time."""
    async def unblock():
        await asyncio.sleep(0.1)
        await r.rpush('list', 'y')

    task = event_loop.create_task(unblock())
    result = await conn.blpop('list', timeout=1)
    assert result == [b'list', b'y']
    await task


@pytest.mark.slow
async def test_blocking_pipeline(conn):
    """Blocking command with another command issued behind it."""
    await conn.set('foo', 'bar')
    fut = asyncio.ensure_future(conn.blpop('list', timeout=1))
    assert (await conn.get('foo')) == b'bar'
    assert (await fut) is None


async def test_wrongtype_error(r):
    await r.set('foo', 'bar')
    with pytest.raises(aioredis.ReplyError, match='^WRONGTYPE'):
        await r.rpush('foo', 'baz')


async def test_syntax_error(r):
    with pytest.raises(aioredis.ReplyError,
                       match="^ERR wrong number of arguments for 'get' command$"):
        await r.execute('get')


async def test_no_script_error(r):
    with pytest.raises(aioredis.ReplyError, match='^NOSCRIPT '):
        await r.evalsha('0123456789abcdef0123456789abcdef')


async def test_failed_script_error(r):
    await r.set('foo', 'bar')
    with pytest.raises(aioredis.ReplyError, match='^ERR Error running script'):
        await r.eval('return redis.call("ZCOUNT", KEYS[1])', ['foo'])
