import pytest_asyncio
import redis

import fakeredis


@pytest_asyncio.fixture(scope="session")
def is_redis_running():
    try:
        r = redis.StrictRedis('localhost', port=6379)
        r.ping()
        return True
    except redis.ConnectionError:
        return False
    finally:
        if hasattr(r, 'close'):
            r.close()  # Absent in older versions of redis-py


@pytest_asyncio.fixture
def fake_server(request):
    server = fakeredis.FakeServer()
    server.connected = request.node.get_closest_marker('disconnected') is None
    return server
