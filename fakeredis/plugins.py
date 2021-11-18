from typing import Type
from unittest import mock
import pytest

import fakeredis


@pytest.fixture
def _fake_redis() -> Type[fakeredis.FakeStrictRedis]:
    with mock.patch("redis.Redis", fakeredis.FakeStrictRedis):
        yield fakeredis.FakeStrictRedis
