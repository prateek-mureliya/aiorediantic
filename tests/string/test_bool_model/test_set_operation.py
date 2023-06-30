import pytest
from typing import Any
from datetime import datetime, timedelta
from packaging.version import Version
from dirty_equals import IsInt, IsPositiveInt


from aiorediantic import (
    RedisClient,
    BoolModel,
    InvalidOptionsCombinationException,
    InvalidArgumentTypeException,
    OldRedisVersionException,
)
from aiorediantic.types import BoolReturn
from tests.conftest import high_version


use_version = Version(high_version)
v2_6_12 = Version("2.6.12")
v6_0_0 = Version("6.0.0")
v6_2_0 = Version("6.2.0")
v7_0_0 = Version("7.0.0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [None, 4542, 3.14, b"bytes", "Hello"],
)
async def testBoolSetOperation_shouldRaiseInvalidArgumentTypeException_whenWrongValueTypePass(
    redis_client: RedisClient, value: Any
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-xx-option-old-version")

    # Act
    with pytest.raises(InvalidArgumentTypeException) as exc_info:
        await obj.set(value)

    # Assert
    excepted = "BoolModel allow to set only BOOL value"
    assert exc_info.type == InvalidArgumentTypeException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testBoolSetOperation_shouldReturn_1_whenValueSet(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set")

    # Act
    actual: BoolReturn = await obj.set(False)

    # Assert
    expected = 1
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_NX_option_shouldReturn_1_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-nx-not-exists")

    # Act
    actual: BoolReturn = await obj.set(False, nx=True)

    # Assert
    expected = 1
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_NX_option_shouldReturn_0_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-nx-exists")
    await obj.client.set(obj.redisKey, "true")  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(False, nx=True)

    # Assert
    expected = 0
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenNXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-nx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, nx=True)

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
async def testBoolSetOperationWith_XX_option_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-xx-exists")
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(False, xx=True)

    # Assert
    expected = 1
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_XX_option_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-xx-not-exists")

    # Act
    actual: BoolReturn = await obj.set(False, xx=True)

    # Assert
    expected = 0
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenXXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-xx-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, xx=True)

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
async def testBoolSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndXXOptionPassTogether(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-nx-xx-options-together")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(False, nx=True, xx=True)

    # Assert
    excepted = "NX and XX options both can not be True together"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testBoolSetOperationWith_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-get-option-key-exists")
    await obj.client.set(obj.redisKey, "false")  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(True, get=True)

    # Assert
    expected = False
    assert actual is expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testBoolSetOperationWith_GET_Option_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-get-option-key-not-exists")

    # Act
    actual: BoolReturn = await obj.set(False, get=True)

    # Assert
    expected = 0
    assert actual == IsInt & expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testBoolSetOperationWith_NX_and_GET_Option_shouldReturnOldValue_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-nx-get-options-key-exists")
    await obj.client.set(obj.redisKey, "true")  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(False, nx=True, get=True)

    # Assert
    expected = True
    assert actual is expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenGETOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-get-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, get=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support GET option. Required version: 6.2.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseInvalidOptionsCombinationException_whenNXAndGetOptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-nx-get-options")

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(False, nx=True, get=True)

    # Assert
    excepted = "Current version: 6.2.0 is not support NX and GET options to be used together. Required version: 7.0.0"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_EX_option_shouldReturn_1_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-ex-option-as-int")

    # Act
    actual: BoolReturn = await obj.set(False, ex=4)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_EX_option_shouldReturn_1_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-ex-option-as-timedelta")

    # Act
    actual: BoolReturn = await obj.set(False, ex=timedelta(minutes=1))
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenEXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-ex-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, ex=2)

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
async def testBoolSetOperationWith_PX_option_shouldReturn_1_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-px-option-as-int")

    # Act
    actual: BoolReturn = await obj.set(False, px=4000)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_12,
    reason="skip test because used version is below 2.6.12 redis version",
)
async def testBoolSetOperationWith_PX_option_shouldReturn_1_whenKeyExTimeAsTimedelta(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-px-option-as-timedelta")

    # Act
    actual: BoolReturn = await obj.set(False, px=timedelta(minutes=1))
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenPXOptionPassInOldRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-px-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, px=2000)

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
async def testBoolSetOperationWith_EXAT_option_shouldReturn_1_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-exat-option-as-int")
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp())

    # Act
    actual: BoolReturn = await obj.set(False, exat=epoch_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testBoolSetOperationWith_EXAT_option_shouldReturn_1_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-exat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: BoolReturn = await obj.set(False, exat=date_time)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenEXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-exat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, exat=date_time)

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
async def testBoolSetOperationWith_PXAT_option_shouldReturn_1_whenKeyExTimeAsInt(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-pxat-option-as-int")
    epoch_time: int = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: BoolReturn = await obj.set(False, pxat=epoch_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_2_0,
    reason="skip test because used version is below 6.2.0 redis version",
)
async def testBoolSetOperationWith_PXAT_option_shouldReturn_1_whenKeyExTimeAsDatetime(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-pxat-option-as-datetime")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: BoolReturn = await obj.set(False, pxat=date_time)
    actualttl = await obj.client.pttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenPXATOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-pxat-option-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, pxat=date_time)

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
async def testBoolSetOperationWith_Keepttl_Option_shouldReturn_1_whenKeepttlAsTrue(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-keepttl-option-as-true")
    await obj.client.set(obj.redisKey, "true", ex=10)  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(False, keepttl=True)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == IsPositiveInt


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v6_0_0,
    reason="skip test because used version is below 6.0.0 redis version",
)
async def testBoolSetOperationWith_Keepttl_Option_shouldReturn_1_whenKeepttlAsFalse(
    redis_client: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-with-keepttl-option-as-false")
    await obj.client.set(obj.redisKey, "true", ex=10)  # pyright: ignore

    # Act
    actual: BoolReturn = await obj.set(False, keepttl=False)
    actualttl = await obj.client.ttl(obj.redisKey)  # pyright: ignore

    # Assert
    expected = 1
    assert actual == IsInt & expected
    assert actualttl == -1

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseOldRedisVersionException_whenKeepttlOptionPassInOldRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-keepttl-option-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.set(False, keepttl=True)

    # Assert
    excepted = (
        "Current version: 2.6.0 is not support KEEPTTL option. Required version: 6.0.0"
    )
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted


@pytest.mark.asyncio
async def testBoolSetOperation_shouldRaiseInvalidOptionsCombinationException_when_EX_PX_EXAT_PXAT_KEEPTTL_OptionPassTogether(
    redis_client_6_2_0: RedisClient,
) -> None:
    # Arrange
    strKey = BoolModel(redisClient=redis_client_6_2_0, keyFormat="{keyname}")
    obj: BoolModel = strKey(keyname="bool-set-ex-px-exat-keepttl-pxat-options-together")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    with pytest.raises(InvalidOptionsCombinationException) as exc_info:
        await obj.set(
            False, ex=2, px=2000, exat=date_time, pxat=date_time, keepttl=True
        )

    # Assert
    excepted = "EX, PX, EXAT, PXAT, KEEPTTL combination are not allow"
    assert exc_info.type == InvalidOptionsCombinationException
    assert exc_info.value.args[0] == excepted
