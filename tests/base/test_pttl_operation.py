import pytest


from aiorediantic import RedisClient, OldRedisVersionException
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version
from packaging.version import Version

use_version = Version(high_version)
v2_6_0 = Version("2.6.0")
v2_8_0 = Version("2.8.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPttlOperation_shouldReturnPttl_whenKeyExistsWithTtl(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-pttl-check")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue", px=2000)  # pyright: ignore

    # Act
    actual: int = await obj.pttl()

    # Assert
    assert type(actual) == int
    assert actual > 0


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPttlOperation_shouldReturn_mins1_whenKeyExistsWithNoTtl(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-pttl-check-nottl")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.pttl()

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
async def testPttlOperation_shouldReturn_mins2_whenKeydoesnotExist(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-pttl-check-does-not-exist")

    # Act
    actual: int = await obj.pttl()

    # Assert
    expected = -2
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testExpireatOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-pttl-check-exception")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.pttl()

    # Assert
    excepted = (
        "Current version: 1.2.0 is not support PTTL operation. Required version: 2.6.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
