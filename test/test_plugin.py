import redis
import fakeredis

pytest_plugins = "fakeredis.plugins"


def test_fake_redis_plugin(_fake_redis):
    assert isinstance(redis.Redis(), fakeredis.FakeStrictRedis)
