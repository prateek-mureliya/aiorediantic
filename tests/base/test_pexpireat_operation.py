import pytest
from datetime import timedelta, datetime
from packaging.version import Version


from aiorediantic import RedisClient, ExpireEnum, OldRedisVersionException
from aiorediantic.base.redis_key import RedisKey
from ..conftest import high_version


use_version = Version(high_version)
v7_0_0 = Version("7.0.0")
v2_6_0 = Version("2.6.0")


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperation_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-key-exists")
    # set a key no expiry
    await obj.client.set("pexpireat-key-exists", "tempvalue")  # pyright: ignore
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: int = await obj.pexpireat(epoch_in_milliseconds=epoch_time)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperation_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-key-not-exists")
    epoch_time = int((datetime.now() + timedelta(seconds=2)).timestamp()) * 1000

    # Act
    actual: int = await obj.pexpireat(epoch_in_milliseconds=epoch_time)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_NX_Option_shouldReturn_1_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-nx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set("pexpireat-nx-key-no-expiry", "tempvalue")  # pyright: ignore
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.NX
    )

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_NX_Option_shouldReturn_0_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-nx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-nx-key-has-expiry", "tempvalue", ex=2
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.NX
    )

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_XX_Option_shouldReturn_1_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-xx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-xx-key-has-expiry", "tempvalue", ex=2
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.XX
    )

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_XX_Option_shouldReturn_0_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-xx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set("pexpireat-xx-key-no-expiry", "tempvalue")  # pyright: ignore
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.XX
    )

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected

    # cleanup
    await obj.delete()


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_GT_Option_shouldReturn_1_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-gt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-gt-key-greater-expiry", "tempvalue", ex=2
    )
    date_time: datetime = datetime.now() + timedelta(seconds=4)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.GT
    )

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_GT_Option_shouldReturn_0_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-gt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-gt-key-less-expiry", "tempvalue", ex=4
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.GT
    )

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_LT_Option_shouldReturn_1_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-lt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-lt-key-less-expiry", "tempvalue", ex=4
    )
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.LT
    )

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireatOperationWith_LT_Option_shouldReturn_0_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-lt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpireat-lt-key-greater-expiry", "tempvalue", ex=2
    )
    date_time: datetime = datetime.now() + timedelta(seconds=4)

    # Act
    actual: int = await obj.pexpireat(
        epoch_in_milliseconds=date_time, option=ExpireEnum.LT
    )

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testPexpireatOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-key-use-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Assert
    excepted = "Current version: 2.6.0 is not support options: NX, XX, GT and LT. Required version: 7.0.0"
    with pytest.raises(OldRedisVersionException, match=excepted):
        # Act
        await obj.pexpireat(epoch_in_milliseconds=date_time, option=ExpireEnum.LT)


@pytest.mark.asyncio
async def testPexpireatOperation_shouldRaiseOldRedisVersionException_whenOperationNotSupportRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpireat-key-use-old-version")
    date_time: datetime = datetime.now() + timedelta(seconds=2)

    # Assert
    excepted = "Current version: 1.2.0 is not support PEXPIREAT operation. Required version: 2.6.0"
    with pytest.raises(OldRedisVersionException, match=excepted):
        # Act
        await obj.pexpireat(epoch_in_milliseconds=date_time)
