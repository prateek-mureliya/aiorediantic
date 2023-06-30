import pytest
from typing import Any
from datetime import datetime, timedelta
from packaging.version import Version
from dirty_equals import IsFloat, IsPositiveInt


from aiorediantic import (
    RedisClient,
    FloatModel,
    InvalidOptionsCombinationException,
    InvalidArgumentTypeException,
    OldRedisVersionException,
)
from aiorediantic.types import FloatReturn
from tests.conftest import high_version


use_version = Version(high_version)
v2_6_12 = Version("2.6.12")
v6_0_0 = Version("6.0.0")
v6_2_0 = Version("6.2.0")
v7_0_0 = Version("7.0.0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [None, "OK", "abc", b"bytes", False, True, 89],
)
async def testFloatSetOperation_shouldRaiseInvalidArgumentTypeException_whenWrongValueTypePass(
    redis_client: RedisClient, value: Any
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-xx-option-old-version")

    # Act
    with pytest.raises(InvalidArgumentTypeException) as exc_info:
        await obj.set(value)

    # Assert
    excepted = "FloatModel allow to set only FLOAT value"
    assert exc_info.type == InvalidArgumentTypeException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testFloatSetOperation_shouldReturnTrue_whenValueSet(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set")

    # Act
    actual: FloatReturn = await obj.set(90.6)

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_NX_option_shouldReturnTrue_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-nx-not-exists")

    # Act
    actual: FloatReturn = await obj.set(20.3, nx=True)

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_NX_option_shouldReturnFalse_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-nx-exists")
    await obj.client.set(obj.redisKey, 10)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(15.78, nx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenNXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-nx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(67.5744, nx=True)

    # Assert
    excepted = (
        "Current version: 1.2.0 is not support NX option. Required version: 2.6.12"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_XX_option_shouldReturnTrue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-xx-exists")
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(56.355, xx=True)

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_XX_option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-xx-not-exists")

    # Act
    actual: FloatReturn = await obj.set(76.9865, xx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenXXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-xx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(675.8765, xx=True)

    # Assert
    excepted = (
        "Current version: 1.2.0 is not support XX option. Required version: 2.6.12"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndXXOptionPassTogether(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-nx-xx-options-together")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(89.57643, nx=True, xx=True)

    # Assert
    excepted = "NX and XX options both can not be True together"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-get-option-key-exists")
    await obj.client.set(obj.redisKey, 34.45)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(78.66336, get=True)

    # Assert
    expected = 34.45
    assert actual == IsFloat & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_GET_Option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-get-option-key-not-exists")

    # Act
    actual: FloatReturn = await obj.set(12.6753, get=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testFloatSetOperationWith_NX_and_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-nx-get-options-key-exists")
    await obj.client.set(obj.redisKey, 67.7766)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(33.67643, nx=True, get=True)

    # Assert
    expected = 67.7766
    assert actual == IsFloat & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenGETOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-get-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(56.00, get=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support GET option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndGetOptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-nx-get-options")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(90.00001, nx=True, get=True)

    # Assert
    excepted = "Current version: 6.2.0 is not support NX and GET options to be used together. Required version: 7.0.0"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-ex-option-as-int")

    # Act
    actual: FloatReturn = await obj.set(45.7786783, ex=4)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-ex-option-as-timedelta")

    # Act
    actual: FloatReturn = await obj.set(76.42792, ex=timedelta(minutes=1))
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenEXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-ex-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(67.9, ex=2)

    # Assert
    excepted = (
        "Current version: 1.2.0 is not support EX option. Required version: 2.6.12"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-px-option-as-int")

    # Act
    actual: FloatReturn = await obj.set(45.90, px=4000)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testFloatSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-px-option-as-timedelta")

    # Act
    actual: FloatReturn = await obj.set(56.65, px=timedelta(minutes=1))
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenPXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-px-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(78.67, px=2000)

    # Assert
    excepted = (
        "Current version: 1.2.0 is not support PX option. Required version: 2.6.12"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-exat-option-as-int")
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp())

    # Act
    actual: FloatReturn = await obj.set(56.45, exat=epoch_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-exat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: FloatReturn = await obj.set(987.0, exat=date_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenEXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-exat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(56575.69, exat=date_time)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support EXAT option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-pxat-option-as-int")
    epoch_time: int = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: FloatReturn = await obj.set(676.23, pxat=epoch_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testFloatSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-pxat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: FloatReturn = await obj.set(89787.46, pxat=date_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenPXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-pxat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(6565.93, pxat=date_time)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support PXAT option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_0_0,
    reason="skip test because used version is below 6.0.0 redis version",
)
async def testFloatSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsTrue(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-keepttl-option-as-true")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(34346.49, keepttl=True)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_0_0,
    reason="skip test because used version is below 6.0.0 redis version",
)
async def testFloatSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsFalse(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-with-keepttl-option-as-false")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: FloatReturn = await obj.set(9532.87, keepttl=False)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == -1

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseOldRedisVersionException_whenKeepttlOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(keyname="float-set-keepttl-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(7278.98, keepttl=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support KEEPTTL option. Required version: 6.0.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testFloatSetOperation_shouldRaiseInvalidOptionsCombinationException_when_EX_PX_EXAT_PXAT_KEEPTTL_OptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = FloatModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: FloatModel = strKey(
        keyname="float-set-ex-px-exat-keepttl-pxat-options-together"
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(
            5467.90, ex=2, px=2000, exat=date_time, pxat=date_time, keepttl=True
        )

    # Assert
    excepted = "EX, PX, EXAT, PXAT, KEEPTTL combination are not allow"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted
