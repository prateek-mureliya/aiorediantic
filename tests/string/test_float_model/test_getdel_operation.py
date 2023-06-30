import pytest
from dirty_equals import IsFloat
from packaging.version import Version


from aiorediantic import (
    RedisClient,
    FloatModel,
    OldRedisVersionException,
    UnexpectedReturnTypeException,
)
from aiorediantic.types import FloatReturn
from tests.conftest import high_version


use_version = Version(high_version)
v6_2_0 = Version("6.2.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatGetDelOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-getdel-exists")
    await obj.client.set(obj.redisKey, 156.56)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.getdel()

    # Assert
    expected = 156.56
    assert actual == IsFloat & expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatGetDelOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-getdel-not-exists")

    # Act
    actual: FloatReturn = await obj.getdel()

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected


@pytest.mark.asyncio
async def testFloatGetDelOperation_shouldRaiseUnexpectedReturnTypeException_whenReturnValueTypeMismatch(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-getdel-return-mismmatch")
    await obj.client.set(obj.redisKey, "xyz")  # pyright: ignore
    # Act
    with pytest.raises(UnexpectedReturnTypeException) as exc_info:
        await obj.get()

    # Assert
    excepted = "FloatModel expect FLOAT return type but get value xyz"
    assert exc_info.type == UnexpectedReturnTypeException
    assert exc_info.value.args[0] == excepted

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testFloatGetDelOperation_shouldRaiseOldRedisVersionException_whenUseOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-getdel-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.getdel()

    # Assert
    excepted = "Current version: 2.6.0 is not support GETDEL operation. Required version: 6.2.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
