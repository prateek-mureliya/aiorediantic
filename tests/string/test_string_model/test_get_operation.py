import pytest
from dirty_equals import IsStr


from aiorediantic import RedisClient, StringModel
from aiorediantic.types import StrReturn


@pytest.mark.asyncio
async def testStringGetOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-get-exists")
    await obj.client.set(obj.redisKey, "test value")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.get()

    # Assert
    expected = "test value"
    assert actual == IsStr & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testStringGetOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-get-not-exists")

    # Act
    actual: StrReturn = await obj.get()

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected
