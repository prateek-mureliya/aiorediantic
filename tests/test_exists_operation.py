import pytest


from aiorediantic import RedisClient
from aiorediantic.base.redis_key import RedisKey


@pytest.mark.asyncio
async def testExistsOperation_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-exists")
    # set a key
    await obj.client.set("key-exists", "tempvalue", ex=5)  # pyright: ignore

    # Act
    actual: int = await obj.exists()

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testExistsOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-not-exists")

    # Act
    actual: int = await obj.exists()

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected
