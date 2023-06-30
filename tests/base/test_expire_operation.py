import pytest
from datetime import timedelta
from packaging.version import Version


from aiorediantic import RedisClient, ExpireEnum, OldRedisVersionException
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version


use_version = Version(high_version)
v7_0_0 = Version("7.0.0")


@pytest.mark.asyncio
async def testExpireOperation_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-key-exists")
    # set a key no expiry
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.expire(seconds=timedelta(minutes=1))

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testExpireOperation_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-key-not-exists")

    # Act
    actual: int = await obj.expire(seconds=timedelta(minutes=1))

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_NX_Option_shouldReturn_1_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-nx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.NX)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_NX_Option_shouldReturn_0_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-nx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-nx-key-has-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.NX)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_XX_Option_shouldReturn_1_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-xx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-xx-key-has-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.XX)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_XX_Option_shouldReturn_0_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-xx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set(obj.redisKey, "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.XX)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected

    # cleanup
    await obj.client.delete(obj.redisKey)  # pyright: ignore


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_GT_Option_shouldReturn_1_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-gt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-gt-key-greater-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.expire(seconds=4, option=ExpireEnum.GT)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_GT_Option_shouldReturn_0_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-gt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-gt-key-less-expiry", "tempvalue", ex=4
    )

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.GT)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_LT_Option_shouldReturn_1_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-lt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-lt-key-less-expiry", "tempvalue", ex=4
    )

    # Act
    actual: int = await obj.expire(seconds=2, option=ExpireEnum.LT)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testExpireOperationWith_LT_Option_shouldReturn_0_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-lt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "expire-lt-key-greater-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.expire(seconds=4, option=ExpireEnum.LT)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testExpireOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="expire-key-use-old-version")

    # Act
    with pytest.raises(OldRedisVersionException) as exc_info:
        await obj.expire(seconds=4)

    # Assert
    excepted = "Current version: 2.6.0 is not support options: NX, XX, GT and LT. Required version: 7.0.0"
    assert exc_info.type == OldRedisVersionException
    assert exc_info.value.args[0] == excepted
