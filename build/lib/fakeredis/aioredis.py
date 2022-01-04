import aioredis
import packaging.version


if packaging.version.Version(aioredis.__version__) >= packaging.version.Version('2.0.0a1'):
    from ._aioredis2 import FakeConnection, FakeRedis  # noqa: F401
else:
    from ._aioredis1 import (  # noqa: F401
        FakeConnectionsPool, create_connection, create_redis, create_pool, create_redis_pool
    )
