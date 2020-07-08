import asyncio

import pytest
import aioredis

import fakeredis.aioredis


@pytest.fixture(params=['fake', 'real'])
def r(request, event_loop):
    if request.param == 'fake':
        ret = event_loop.run_until_complete(fakeredis.aioredis.create_redis_pool())
    else:
        if not request.getfixturevalue('is_redis_running'):
            pytest.skip('Redis is not running')
        ret = event_loop.run_until_complete(aioredis.create_redis_pool('redis://localhost'))
    event_loop.run_until_complete(ret.flushall())
    yield ret
    event_loop.run_until_complete(ret.flushall())
    ret.close()
    event_loop.run_until_complete(ret.wait_closed())


@pytest.fixture
def conn(r, event_loop):
    """A single connection, rather than a pool."""
    conn = event_loop.run_until_complete(r)
    with conn:
        yield conn


@pytest.mark.asyncio
async def test_ping(r):
    pong = await r.ping()
    assert pong == b'PONG'


@pytest.mark.asyncio
async def test_types(r):
    await r.hmset_dict('hash', key1='value1', key2='value2', key3=123)
    result = await r.hgetall('hash', encoding='utf-8')
    assert result == {
        'key1': 'value1',
        'key2': 'value2',
        'key3': '123'
    }


@pytest.mark.asyncio
async def test_transaction(r):
    tr = r.multi_exec()
    tr.set('key1', 'value1')
    tr.set('key2', 'value2')
    ok1, ok2 = await tr.execute()
    assert ok1
    assert ok2
    result = await r.get('key1')
    assert result == b'value1'


@pytest.mark.asyncio
async def test_transaction_fail(r, conn):
    # ensure that the WATCH applies to the same connection as the MULTI/EXEC.
    await r.set('foo', '1')
    await conn.watch('foo')
    await conn.set('foo', '2')    # Different connection
    tr = conn.multi_exec()
    tr.get('foo')
    with pytest.raises(aioredis.MultiExecError):
        await tr.execute()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_blocking_ready(r, conn):
    """Blocking command which does not need to block."""
    await r.rpush('list', 'x')
    result = await conn.blpop('list', timeout=1)
    assert result == [b'list', b'x']


@pytest.mark.asyncio
@pytest.mark.slow
async def test_blocking_timeout(conn):
    """Blocking command that times out without completing."""
    result = await conn.blpop('missing', timeout=1)
    assert result is None


@pytest.mark.asyncio
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


@pytest.mark.asyncio
@pytest.mark.slow
async def test_blocking_pipeline(conn):
    """Blocking command with another command issued behind it."""
    await conn.set('foo', 'bar')
    fut = asyncio.ensure_future(conn.blpop('list', timeout=1))
    assert (await conn.get('foo')) == b'bar'
    assert (await fut) is None


@pytest.mark.asyncio
async def test_wrongtype_error(r):
    await r.set('foo', 'bar')
    with pytest.raises(aioredis.ReplyError, match='^WRONGTYPE'):
        await r.rpush('foo', 'baz')


@pytest.mark.asyncio
async def test_syntax_error(r):
    with pytest.raises(aioredis.ReplyError,
                       match="^ERR wrong number of arguments for 'get' command$"):
        await r.execute('get')


@pytest.mark.asyncio
async def test_no_script_error(r):
    with pytest.raises(aioredis.ReplyError, match='^NOSCRIPT '):
        await r.evalsha('0123456789abcdef0123456789abcdef')


@pytest.mark.asyncio
async def test_failed_script_error(r):
    await r.set('foo', 'bar')
    with pytest.raises(aioredis.ReplyError, match='^ERR Error running script'):
        await r.eval('return redis.call("ZCOUNT", KEYS[1])', ['foo'])
