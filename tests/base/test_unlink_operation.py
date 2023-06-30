import pytest


from aiorediantic import RedisClient, OldRedisVersionException
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version
from packaging.version import Version

use_version = Version(high_version)
v2_8_0 = Version("2.8.0")
v4_0_0 = Version("4.0.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v4_0_0,
    reason="skip test because used version is below 4.0.0 redis version",
)
async def testUnlinkOperation_shouldReturnInteger_whenKeyPresent(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-unlink-check")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue", ex=2)  # pyright: ignore

    # Act
    actual: int = await obj.unlink()

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v4_0_0,
    reason="skip test because used version is below 4.0.0 redis version",
)
async def testUnlinkOperation_shouldReturnInteger_whenKeyNotPresent(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-unlink-check-key-not-exist")
    # set a key
    # await obj.client.set(obj.redisKey, "tempvalue", ex=2)  # pyright: ignore

    # Act
    actual: int = await obj.unlink()

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testUnlinkatOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-unlink-check-exception")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.unlink()

    # Assert
    excepted = "Current version: 2.6.0 is not support UNLINK operation. Required version: 4.0.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
