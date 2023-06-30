import pytest
from dirty_equals import IsStr
from packaging.version import Version


from aiorediantic import RedisClient, StringModel, OldRedisVersionException
from aiorediantic.types import StrReturn
from tests.conftest import high_version


use_version = Version(high_version)
v6_2_0 = Version("6.2.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testStringGetDelOperation_shouldReturnValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-getdel-exists")
    await obj.client.set(obj.redisKey, "test value")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.getdel()

    # Assert
    expected = "test value"
    assert actual == IsStr & expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testStringGetDelOperation_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-getdel-not-exists")

    # Act
    actual: StrReturn = await obj.getdel()

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected


@pytest.mark.asyncio
async def testStringGetDelOperation_shouldRaiseOldRedisVersionException_whenUseOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-getdel-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.getdel()

    # Assert
    excepted = "Current version: 2.6.0 is not support GETDEL operation. Required version: 6.2.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
