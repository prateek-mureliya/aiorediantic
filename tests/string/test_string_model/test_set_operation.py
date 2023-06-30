import pytest
from typing import Any
from datetime import datetime, timedelta
from packaging.version import Version
from dirty_equals import IsStr, IsPositiveInt


from aiorediantic import (
    RedisClient,
    StringModel,
    InvalidOptionsCombinationException,
    InvalidArgumentTypeException,
    OldRedisVersionException,
)
from aiorediantic.types import StrReturn
from tests.conftest import high_version


use_version = Version(high_version)
v2_6_12 = Version("2.6.12")
v6_0_0 = Version("6.0.0")
v6_2_0 = Version("6.2.0")
v7_0_0 = Version("7.0.0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [None, 4542, 3.14, b"bytes", False, True],
)
async def testStringSetOperation_shouldRaiseInvalidArgumentTypeException_whenWrongValueTypePass(
    redis_client: RedisClient, value: Any
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="str-set-xx-option-old-version")

    # Act
    with pytest.raises(InvalidArgumentTypeException) as exc_info:
        await obj.set(value)

    # Assert
    excepted = "StringModel allow to set only STRING value"
    assert exc_info.type == InvalidArgumentTypeException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testStringSetOperation_shouldReturnTrue_whenValueSet(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set")

    # Act
    actual: StrReturn = await obj.set("tempval")

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
async def testStringSetOperationWith_NX_option_shouldReturnTrue_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-nx-not-exists")

    # Act
    actual: StrReturn = await obj.set("tempval", nx=True)

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
async def testStringSetOperationWith_NX_option_shouldReturnFalse_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-nx-exists")
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("tempval", nx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenNXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-nx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", nx=True)

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
async def testStringSetOperationWith_XX_option_shouldReturnTrue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-xx-exists")
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("tempval", xx=True)

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
async def testStringSetOperationWith_XX_option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-xx-not-exists")

    # Act
    actual: StrReturn = await obj.set("tempval", xx=True)

    # Assert
    expected = False
    assert type(actual) == bool
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenXXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-xx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", xx=True)

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
async def testStringSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndXXOptionPassTogether(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-nx-xx-options-together")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set("tempval", nx=True, xx=True)

    # Assert
    excepted = "NX and XX options both can not be True together"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testStringSetOperationWith_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-get-option-key-exists")
    await obj.client.set(obj.redisKey, "old value")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("new value", get=True)

    # Assert
    expected = "old value"
    assert actual == IsStr & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testStringSetOperationWith_GET_Option_shouldReturnFalse_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-get-option-key-not-exists")

    # Act
    actual: StrReturn = await obj.set("tempval", get=True)

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
async def testStringSetOperationWith_NX_and_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-nx-get-options-key-exists")
    await obj.client.set(obj.redisKey, "old value")  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("new value", nx=True, get=True)

    # Assert
    expected = "old value"
    assert actual == IsStr & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenGETOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-get-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", get=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support GET option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndGetOptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-nx-get-options")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set("tempval", nx=True, get=True)

    # Assert
    excepted = "Current version: 6.2.0 is not support NX and GET options to be used together. Required version: 7.0.0"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testStringSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-ex-option-as-int")

    # Act
    actual: StrReturn = await obj.set("tempval", ex=4)
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
async def testStringSetOperationWith_EX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-ex-option-as-timedelta")

    # Act
    actual: StrReturn = await obj.set("tempval", ex=timedelta(minutes=1))
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenEXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-ex-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", ex=2)

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
async def testStringSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-px-option-as-int")

    # Act
    actual: StrReturn = await obj.set("tempval", px=4000)
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
async def testStringSetOperationWith_PX_option_shouldReturnTrue_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-px-option-as-timedelta")

    # Act
    actual: StrReturn = await obj.set("tempval", px=timedelta(minutes=1))
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenPXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-px-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", px=2000)

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
async def testStringSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-exat-option-as-int")
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp())

    # Act
    actual: StrReturn = await obj.set("tempval", exat=epoch_time)
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
async def testStringSetOperationWith_EXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-exat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: StrReturn = await obj.set("tempval", exat=date_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenEXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-exat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", exat=date_time)

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
async def testStringSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-pxat-option-as-int")
    epoch_time: int = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: StrReturn = await obj.set("tempval", pxat=epoch_time)
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
async def testStringSetOperationWith_PXAT_option_shouldReturnTrue_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-pxat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: StrReturn = await obj.set("tempval", pxat=date_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenPXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-pxat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", pxat=date_time)

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
async def testStringSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsTrue(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-keepttl-option-as-true")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("tempval", keepttl=True)
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
async def testStringSetOperationWith_Keepttl_Option_shouldReturnTrue_whenKeepttlAsFalse(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-with-keepttl-option-as-false")
    await obj.client.set(obj.redisKey, "tempvalue", ex=10)  # pyright: ignore

    # Act
    actual: StrReturn = await obj.set("tempval", keepttl=False)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = True
    assert type(actual) == bool
    assert actual == expected
    assert actualttl == -1

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseOldRedisVersionException_whenKeepttlOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: StringModel = strKey(keyname="string-set-keepttl-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set("tempval", keepttl=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support KEEPTTL option. Required version: 6.0.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testStringSetOperation_shouldRaiseInvalidOptionsCombinationException_when_EX_PX_EXAT_PXAT_KEEPTTL_OptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = StringModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: StringModel = strKey(
        keyname="string-set-ex-px-exat-keepttl-pxat-options-together"
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(
            "tempval", ex=2, px=2000, exat=date_time, pxat=date_time, keepttl=True
        )

    # Assert
    excepted = "EX, PX, EXAT, PXAT, KEEPTTL combination are not allow"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted
