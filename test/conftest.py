import pytest
import redis

import fakeredis


@pytest.fixture(scope="session")
def is_redis_running():
    try:
        r = redis.StrictRedis('localhost', port=6379)
        r.ping()
        return True
    except redis.ConnectionError:
        return False
    finally:
        if hasattr(r, 'close'):
            r.close()      # Absent in older versions of redis-py


@pytest.fixture
def fake_server(request):
    server = fakeredis.FakeServer()
    server.connected = request.node.get_closest_marker('disconnected') is None
    return server
