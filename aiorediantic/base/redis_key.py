from typing import Any, Optional, List
from aioredis.client import ExpiryT, AbsExpiryT, FieldT
from packaging.version import Version

from .redis_model import RedisModel
from ..enum import ExpireEnum
from ..exception import OldRedisVersionException
from ..utils import (
    timedetla_to_seconds,
    timedetla_to_milliseconds,
    unix_to_seconds,
    unix_to_milliseconds,
)


version_7_0_0: Version = Version("7.0.0")
version_2_6_0: Version = Version("2.6.0")


class RedisKey(RedisModel):
    async def delete(self) -> int:
        """
        Removes the current object key.
        Return 1 if key is exists or 0 if key is not exists.
        """
        return await self.client.delete(self.redisKey)  # pyright: ignore

    async def exists(self) -> int:
        """
        Return 1 if key is exists or 0 if key is not exists.
        """
        return await self.client.exists(self.redisKey)  # pyright: ignore

    async def expire(
        self, seconds: ExpiryT, option: Optional[ExpireEnum] = None
    ) -> int:
        """
        Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
        seconds can be represented by an integer or a Python timedelta object.

        Return
            1 if the timeout was set.
            0 if the timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.

        The EXPIRE command supports a set of options:
            NX -- Set expiry only when the key has no expiry
            XX -- Set expiry only when the key has an existing expiry
            GT -- Set expiry only when the new expiry is greater than current one
            LT -- Set expiry only when the new expiry is less than current one

        History
        Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
        """
        if option is not None and self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        seconds = timedetla_to_seconds(seconds)
        pieces: List[FieldT] = [seconds]
        if option:
            pieces.append(option.value)

        status: Any = await self.client.execute_command(  # pyright: ignore
            "EXPIRE", self.redisKey, *pieces
        )
        return 1 if status else 0

    async def expireat(
        self, epoch_in_seconds: AbsExpiryT, option: Optional[ExpireEnum] = None
    ) -> int:
        """
        Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
        epoch_in_seconds can be represented by an integer or a Python datetime object.

        Return
            1 if the timeout was set.
            0 if the timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.

        The EXPIREAT command supports a set of options:
            NX -- Set expiry only when the key has no expiry
            XX -- Set expiry only when the key has an existing expiry
            GT -- Set expiry only when the new expiry is greater than current one
            LT -- Set expiry only when the new expiry is less than current one

        History
        Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
        """
        if option is not None and self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        epoch_in_seconds = unix_to_seconds(epoch_in_seconds)
        pieces: List[FieldT] = [epoch_in_seconds]
        if option:
            pieces.append(option.value)

        status: Any = await self.client.execute_command(  # pyright: ignore
            "EXPIREAT", self.redisKey, *pieces
        )
        return 1 if status else 0

    async def pexpire(
        self, milliseconds: ExpiryT, option: Optional[ExpireEnum] = None
    ) -> int:
        """
        Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
        milliseconds can be represented by an integer or a Python timedelta object.

        Return
            1 if the timeout was set.
            0 if the timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.

        The PEXPIRE command supports a set of options:
            NX -- Set expiry only when the key has no expiry
            XX -- Set expiry only when the key has an existing expiry
            GT -- Set expiry only when the new expiry is greater than current one
            LT -- Set expiry only when the new expiry is less than current one

        History
        Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
        """
        if self.redisVersion < version_2_6_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PEXPIRE operation. Required version: {version_2_6_0}"
            )

        if option is not None and self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        milliseconds = timedetla_to_milliseconds(milliseconds)
        pieces: List[FieldT] = [milliseconds]
        if option:
            pieces.append(option.value)

        return await self.client.execute_command(  # pyright: ignore
            "PEXPIRE", self.redisKey, *pieces
        )

    async def pexpireat(
        self, epoch_in_milliseconds: AbsExpiryT, option: Optional[ExpireEnum] = None
    ) -> int:
        """
        Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
        epoch_in_milliseconds can be represented by an integer or a Python datetime object.

        Return
            1 if the timeout was set.
            0 if the timeout was not set. e.g. key doesn't exist, or operation skipped due to the provided arguments.

        The PEXPIREAT command supports a set of options:
            NX -- Set expiry only when the key has no expiry
            XX -- Set expiry only when the key has an existing expiry
            GT -- Set expiry only when the new expiry is greater than current one
            LT -- Set expiry only when the new expiry is less than current one

        History
        Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
        """
        if self.redisVersion < version_2_6_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PEXPIREAT operation. Required version: {version_2_6_0}"
            )

        if option is not None and self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        epoch_in_milliseconds = unix_to_milliseconds(epoch_in_milliseconds)
        pieces: List[FieldT] = [epoch_in_milliseconds]
        if option:
            pieces.append(option.value)

        return await self.client.execute_command(  # pyright: ignore
            "PEXPIREAT", self.redisKey, *pieces
        )
