import pytest
from dirty_equals import IsInt


from aiorediantic import RedisClient, BoolModel, UnexpectedReturnTypeException
from aiorediantic.types import BoolReturn


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    ["true", "false"],
)
async def testBoolGetOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient, value: str
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-get-exists")
    await obj.client.set(obj.redisKey, value)  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.get()

    # Assert
    expected: bool = value == "true"
    assert actual is expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testBoolGetOperation_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-get-not-exists")

    # Act
    actual: BoolReturn = await obj.get()

    # Assert
    expected = 0
    assert actual == IsInt & expected


@pytest.mark.asyncio
async def testBoolGetOperation_shouldRaiseUnexpectedReturnTypeException_whenReturnValueTypeMismatch(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-get-return-mismatch")
    await obj.client.set(  # pyright: ignore
        obj.redisKey, "2018-12-01T02:03:04.000009+10:30"
    )
    # Act
    with pytest.raises(UnexpectedReturnTypeException) as exc_info:
        await obj.get()

    # Assert
    excepted = "BoolModel expect BOOL return type but get value 2018-12-01T02:03:04.000009+10:30"
    assert exc_info.type == UnexpectedReturnTypeException
    assert exc_info.value.args[0] == excepted

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore
