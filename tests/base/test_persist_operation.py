import pytest


from aiorediantic import RedisClient, OldRedisVersionException
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version
from packaging.version import Version

use_version = Version(high_version)
v2_2_0 = Version("2.2.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_2_0,
    reason="skip test because used version is below 2.2.0 redis version",
)
async def testPersistOperation_shouldReturn1_whenTimeoutRemoved(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-persist-check")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue", ex=2)  # pyright: ignore

    # Act
    actual: int = await obj.persist()

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_2_0,
    reason="skip test because used version is below 2.2.0 redis version",
)
async def testPersistOperation_shouldReturn0_whenTimeoutNotRemovedOrKeyDoesNotExist(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-persist-check-key-not-exist")
    # set a key
    # await obj.client.set(obj.redisKey, "tempvalue", ex=2)  # pyright: ignore

    # Act
    actual: int = await obj.persist()

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testPersistatOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-unlink-check-exception")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.persist()

    # Assert
    excepted = "Current version: 1.2.0 is not support PERSIST operation. Required version: 2.2.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
