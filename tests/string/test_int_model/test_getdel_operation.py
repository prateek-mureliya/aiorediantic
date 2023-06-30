import pytest
from dirty_equals import IsInt
from packaging.version import Version


from aiorediantic import (
    RedisClient,
    IntModel,
    OldRedisVersionException,
    UnexpectedReturnTypeException,
)
from aiorediantic.types import IntReturn
from tests.conftest import high_version


use_version = Version(high_version)
v6_2_0 = Version("6.2.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testIntGetDelOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-getdel-exists")
    await obj.client.set(obj.redisKey, 156)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.getdel()

    # Assert
    expected = 156
    assert actual == IsInt & expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testIntGetDelOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-getdel-not-exists")

    # Act
    actual: IntReturn = await obj.getdel()

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testIntGetDelOperation_shouldRaiseUnexpectedReturnTypeException_whenReturnValueTypeMismatch(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-getdel-return-mismmatch")
    await obj.client.set(obj.redisKey, 3.14)  # pyright: ignore
    # Act
    with pytest.raises(UnexpectedReturnTypeException) as exc_info:
        await obj.getdel()

    # Assert
    excepted = "IntModel expect INT return type but get value 3.14"
    assert exc_info.type == UnexpectedReturnTypeException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testIntGetDelOperation_shouldRaiseOldRedisVersionException_whenUseOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-getdel-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.getdel()

    # Assert
    excepted = "Current version: 2.6.0 is not support GETDEL operation. Required version: 6.2.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
