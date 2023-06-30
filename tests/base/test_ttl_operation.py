import pytest


from aiorediantic import RedisClient
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version
from packaging.version import Version

use_version = Version(high_version)
v2_8_0 = Version("2.8.0")


@pytest.mark.asyncio
async def testTtlOperation_shouldReturnTtl_whenKeyExistsWithTtl(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-ttl-check")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue", ex=2)  # pyright: ignore

    # Act
    actual: int = await obj.ttl()

    # Assert
    expected = 2
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testTtlOperation_shouldReturn_mins1_whenKeyExistsWithNoTtl(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-ttl-check-nottl")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.ttl()

    # Assert
    expected = -1
    assert type(actual) == int
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_8_0,
    reason="skip test because used version is below 2.8.0 redis version",
)
async def testTtlOperation_shouldReturn_mins2_whenKeydoesnotExist(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-ttl-check-does-not-exist")

    # Act
    actual: int = await obj.ttl()

    # Assert
    expected = -2
    assert type(actual) == int
    assert actual == expected
