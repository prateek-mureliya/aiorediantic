import pytest
from dirty_equals import IsInt


from aiorediantic import RedisClient, IntModel, UnexpectedReturnTypeException
from aiorediantic.types import IntReturn


@pytest.mark.asyncio
async def testIntGetOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-get-exists")
    await obj.client.set(obj.redisKey, 786)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.get()

    # Assert
    expected = 786
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testIntGetOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-get-not-exists")

    # Act
    actual: IntReturn = await obj.get()

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected


@pytest.mark.asyncio
async def testIntGetOperation_shouldRaiseUnexpectedReturnTypeException_whenReturnValueTypeMismatch(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-get-return-mismatch")
    await obj.client.set(  # pyright: ignore
        obj.redisKey, "2018-12-01T02:03:04.000009+10:30"
    )
    # Act
    with pytest.raises(UnexpectedReturnTypeException) as exc_info:
        await obj.get()

    # Assert
    excepted = (
        "IntModel expect INT return type but get value 2018-12-01T02:03:04.000009+10:30"
    )
    assert exc_info.type == UnexpectedReturnTypeException
    assert exc_info.value.args[0] == excepted

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore
