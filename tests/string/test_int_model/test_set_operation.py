import pytest
from typing import Any
from datetime import datetime, timedelta
from packaging.version import Version
from dirty_equals import IsInt, IsPositiveInt


from aiorediantic import (
    RedisClient,
    IntModel,
    InvalidOptionsCombinationException,
    InvalidArgumentTypeException,
    OldRedisVersionException,
)
from aiorediantic.types import IntReturn
from tests.conftest import high_version


use_version = Version(high_version)
v2_6_12 = Version("2.6.12")
v6_0_0 = Version("6.0.0")
v6_2_0 = Version("6.2.0")
v7_0_0 = Version("7.0.0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [None, "OK", "abc", b"bytes", False, True, 89.654],
)
async def testIntSetOperation_shouldRaiseInvalidArgumentTypeException_whenWrongValueTypePass(
    redis_client: RedisClient, value: Any
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-xx-option-old-version")

    # Act
    with pytest.raises(InvalidArgumentTypeException) as exc_info:
        await obj.set(value)

    # Assert
    excepted = "IntModel allow to set only INT value"
    assert exc_info.type == InvalidArgumentTypeException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testIntSetOperation_shouldReturnTrue_whenValueSet(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set")

    # Act
    actual: IntReturn = await obj.set(90)

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
async def testIntSetOperationWith_NX_option_shouldReturnTrue_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-nx-not-exists")

    # Act
    actual: IntReturn = await obj.set(20, nx=True)

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
async def testIntSetOperationWith_NX_option_shouldReturnFalse_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-nx-exists")
    await obj.client.set(obj.redisKey, 10)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(15, nx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenNXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-nx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(67, nx=True)

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
async def testIntSetOperationWith_XX_option_shouldReturnTrue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-xx-exists")
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(56, xx=True)

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
async def testIntSetOperationWith_XX_option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-xx-not-exists")

    # Act
    actual: IntReturn = await obj.set(76, xx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenXXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-xx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(675, xx=True)

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
async def testIntSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndXXOptionPassTogether(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-nx-xx-options-together")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(89, nx=True, xx=True)

    # Assert
    excepted = "NX and XX options both can not be True together"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testIntSetOperationWith_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-get-option-key-exists")
    await obj.client.set(obj.redisKey, 34)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(78, get=True)

    # Assert
    expected = 34
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testIntSetOperationWith_GET_Option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-get-option-key-not-exists")

    # Act
    actual: IntReturn = await obj.set(12, get=True)

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
async def testIntSetOperationWith_NX_and_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-nx-get-options-key-exists")
    await obj.client.set(obj.redisKey, 67)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(33, nx=True, get=True)

    # Assert
    expected = 67
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenGETOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-get-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(56, get=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support GET option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndGetOptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-nx-get-options")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(90, nx=True, get=True)

    # Assert
    excepted = "Current version: 6.2.0 is not support NX and GET options to be used together. Required version: 7.0.0"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testIntSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-ex-option-as-int")

    # Act
    actual: IntReturn = await obj.set(45, ex=4)
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
async def testIntSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-ex-option-as-timedelta")

    # Act
    actual: IntReturn = await obj.set(76, ex=timedelta(minutes=1))
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenEXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-ex-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(67, ex=2)

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
async def testIntSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-px-option-as-int")

    # Act
    actual: IntReturn = await obj.set(45, px=4000)
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
async def testIntSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-px-option-as-timedelta")

    # Act
    actual: IntReturn = await obj.set(56, px=timedelta(minutes=1))
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenPXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-px-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(78, px=2000)

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
async def testIntSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-exat-option-as-int")
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp())

    # Act
    actual: IntReturn = await obj.set(56, exat=epoch_time)
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
async def testIntSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-exat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: IntReturn = await obj.set(987, exat=date_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenEXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-exat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(56575, exat=date_time)

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
async def testIntSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-pxat-option-as-int")
    epoch_time: int = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: IntReturn = await obj.set(676, pxat=epoch_time)
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
async def testIntSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-pxat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: IntReturn = await obj.set(89787, pxat=date_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenPXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-pxat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(6565, pxat=date_time)

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
async def testIntSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsTrue(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-keepttl-option-as-true")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(34346, keepttl=True)
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
async def testIntSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsFalse(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-with-keepttl-option-as-false")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: IntReturn = await obj.set(9532, keepttl=False)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == -1

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseOldRedisVersionException_whenKeepttlOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-keepttl-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(7278, keepttl=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support KEEPTTL option. Required version: 6.0.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testIntSetOperation_shouldRaiseInvalidOptionsCombinationException_when_EX_PX_EXAT_PXAT_KEEPTTL_OptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = IntModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: IntModel = strKey(keyname="int-set-ex-px-exat-keepttl-pxat-options-together")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(5467, ex=2, px=2000, exat=date_time, pxat=date_time, keepttl=True)

    # Assert
    excepted = "EX, PX, EXAT, PXAT, KEEPTTL combination are not allow"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted
