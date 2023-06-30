import pytest
from dirty_equals import IsInt
from packaging.version import Version


from aiorediantic import (
    RedisClient,
    BoolModel,
    OldRedisVersionException,
    UnexpectedReturnTypeException,
)
from aiorediantic.types import BoolReturn
from tests.conftest import high_version


use_version = Version(high_version)
v6_2_0 = Version("6.2.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
@pytest.mark.parametrize(
    "value",
    ["true", "false"],
)
async def testBoolGetDelOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient, value: str
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-getdel-exists")
    await obj.client.set(obj.redisKey, value)  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.getdel()

    # Assert
    expected: bool = value == "true"
    assert actual is expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testBoolGetDelOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-getdel-not-exists")

    # Act
    actual: BoolReturn = await obj.getdel()

    # Assert
    expected = 0
    assert actual == IsInt & expected


@pytest.mark.asyncio
async def testBoolGetDelOperation_shouldRaiseOldRedisVersionException_whenUseOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-getdel-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.getdel()

    # Assert
    excepted = "Current version: 2.6.0 is not support GETDEL operation. Required version: 6.2.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testBoolGetOperation_shouldRaiseUnexpectedReturnTypeException_whenReturnValueTypeMismatch(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-getdel-return-mismatch")
    await obj.client.set(obj.redisKey, 789)  # pyright: ignore
    # Act
    with pytest.raises(UnexpectedReturnTypeException) as exc_info:
        await obj.get()

    # Assert
    excepted = "BoolModel expect BOOL return type but get value 789"
    assert exc_info.type == UnexpectedReturnTypeException
    assert exc_info.value.args[0] == excepted

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore
