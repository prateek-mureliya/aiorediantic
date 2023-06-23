from typing import Any, Optional, List
from aioredis.client import ExpiryT, AbsExpiryT, FieldT

from .redis_model import RedisModel
from .redis_version import RedisVersion
from ..enum import ExpireEnum
from ..exception import OldRedisVersionException
from ..utils import (
    timedetla_to_seconds,
    timedetla_to_milliseconds,
    unix_to_seconds,
    unix_to_milliseconds,
)


version_7_0_0: RedisVersion = RedisVersion(major=7, minor=0, patch=0)
version_2_6_0: RedisVersion = RedisVersion(major=2, minor=6, patch=0)


class RedisKey(RedisModel):
    async def delete(self) -> bool:
        status: Any = await self.client.delete(self.redisKey)  # pyright: ignore
        return status > 0

    async def exists(self) -> bool:
        status: Any = await self.client.exists(self.redisKey)  # pyright: ignore
        return status > 0

    async def expire(
        self, seconds: ExpiryT, option: Optional[ExpireEnum] = None
    ) -> bool:
        if self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        seconds = timedetla_to_seconds(seconds)
        pieces: List[FieldT] = [seconds]
        if option:
            pieces.append(option.value)

        return await self.client.execute_command(  # pyright: ignore
            "EXPIRE", self.redisKey, *pieces
        )

    async def expireat(
        self, epoch_in_seconds: AbsExpiryT, option: Optional[ExpireEnum] = None
    ) -> bool:
        if self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        epoch_in_seconds = unix_to_seconds(epoch_in_seconds)
        pieces: List[FieldT] = [epoch_in_seconds]
        if option:
            pieces.append(option.value)

        return await self.client.execute_command(  # pyright: ignore
            "EXPIREAT", self.redisKey, *pieces
        )

    async def pexpire(
        self, milliseconds: ExpiryT, option: Optional[ExpireEnum] = None
    ) -> bool:
        if self.redisVersion < version_2_6_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PEXPIRE operation. Required version: {version_7_0_0}"
            )

        if self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        milliseconds = timedetla_to_milliseconds(milliseconds)
        pieces: List[FieldT] = [milliseconds]
        if option:
            pieces.append(option.value)

        status: Any = await self.client.execute_command(  # pyright: ignore
            "PEXPIRE", self.redisKey, *pieces
        )

        return status == 1

    async def pexpireat(
        self, epoch_in_milliseconds: AbsExpiryT, option: Optional[ExpireEnum] = None
    ) -> bool:
        if self.redisVersion < version_2_6_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PEXPIREAT operation. Required version: {version_7_0_0}"
            )

        if self.redisVersion < version_7_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support options: NX, XX, GT and LT. Required version: {version_7_0_0}",
            )

        epoch_in_milliseconds = unix_to_milliseconds(epoch_in_milliseconds)
        pieces: List[FieldT] = [epoch_in_milliseconds]
        if option:
            pieces.append(option.value)

        status: Any = await self.client.execute_command(  # pyright: ignore
            "PEXPIREAT", self.redisKey, *pieces
        )

        return status == 1
