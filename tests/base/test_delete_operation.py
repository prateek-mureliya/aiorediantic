import pytest


from aiorediantic import RedisClient
from aiorediantic.base.redis_key import RedisKey


@pytest.mark.asyncio
async def testDeleteOperation_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-delete-exists")
    # set a key
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.delete()

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testDeleteOperation_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="key-delete-not-exists")

    # Act
    actual: int = await obj.delete()

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected
