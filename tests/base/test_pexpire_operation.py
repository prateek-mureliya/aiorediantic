import pytest
from datetime import timedelta
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
async def testPexpireOperation_shouldReturn_1_whenKeyExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-key-exists")
    # set a key no expiry
    await obj.client.set("pexpire-key-exists", "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.pexpire(milliseconds=timedelta(minutes=1))

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v2_6_0,
    reason="skip test because used version is below 2.6.0 redis version",
)
async def testPexpireOperation_shouldReturn_0_whenKeyNotExists(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-key-not-exists")

    # Act
    actual: int = await obj.pexpire(milliseconds=timedelta(minutes=1))

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_NX_Option_shouldReturn_1_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-nx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set("pexpire-nx-key-no-expiry", "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.NX)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_NX_Option_shouldReturn_0_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-nx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-nx-key-has-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.NX)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_XX_Option_shouldReturn_1_whenKeyHasExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-xx-key-has-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-xx-key-has-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.XX)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_XX_Option_shouldReturn_0_whenKeyHasNoExpiry(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-xx-key-no-expiry")
    # set a key with no expiry
    await obj.client.set("pexpire-xx-key-no-expiry", "tempvalue")  # pyright: ignore

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.XX)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected

    # cleanup
    await obj.delete()


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_GT_Option_shouldReturn_1_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-gt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-gt-key-greater-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=4000, option=ExpireEnum.GT)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_GT_Option_shouldReturn_0_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-gt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-gt-key-less-expiry", "tempvalue", ex=4
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.GT)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_LT_Option_shouldReturn_1_whenNewExpiryIsLessThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-lt-key-less-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-lt-key-less-expiry", "tempvalue", ex=4
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=2000, option=ExpireEnum.LT)

    # Assert
    expected = 1
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
@pytest.mark.skipif(
    use_version < v7_0_0,
    reason="skip test because used version is below 7.0.0 redis version",
)
async def testPexpireOperationWith_LT_Option_shouldReturn_0_whenNewExpiryIsGreaterThanCurrentOne(
    redis_client: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-lt-key-greater-expiry")
    # set a key with expiry
    await obj.client.set(  # pyright: ignore
        "pexpire-lt-key-greater-expiry", "tempvalue", ex=2
    )

    # Act
    actual: int = await obj.pexpire(milliseconds=4000, option=ExpireEnum.LT)

    # Assert
    expected = 0
    assert type(actual) == int
    assert actual == expected


@pytest.mark.asyncio
async def testPexpireOperation_shouldRaiseOldRedisVersionException_whenOptionNotSupportRedisVersion(
    redis_client_2_6_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_2_6_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-key-use-old-version")

    # Assert
    excepted = "Current version: 2.6.0 is not support options: NX, XX, GT and LT. Required version: 7.0.0"
    with pytest.raises(OldRedisVersionException, match=excepted):
        # Act
        await obj.pexpire(milliseconds=2, option=ExpireEnum.GT)


@pytest.mark.asyncio
async def testPexpireOperation_shouldRaiseOldRedisVersionException_whenOperationNotSupportRedisVersion(
    redis_client_1_2_0: RedisClient,
) -> None:
    # Arrange
    key = RedisKey(redisClient=redis_client_1_2_0, keyFormat="{keyname}")
    obj: RedisKey = key(keyname="pexpire-key-use-old-version")

    # Assert
    excepted = "Current version: 1.2.0 is not support PEXPIRE operation. Required version: 2.6.0"
    with pytest.raises(OldRedisVersionException, match=excepted):
        # Act
        await obj.pexpire(milliseconds=2)
