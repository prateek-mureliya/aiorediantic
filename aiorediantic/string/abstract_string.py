from typing import Any, Optional, List
from packaging.version import Version


from aiorediantic.types import StrBytesT, ExpiryT, FieldT, AbsExpiryT
from aiorediantic.base.redis_key import RedisKey
from aiorediantic.exception import (
    InvalidOptionsCombinationException,
    OldRedisVersionException,
)
from aiorediantic.utils import (
    timedetla_to_seconds,
    timedetla_to_milliseconds,
    unix_to_seconds,
    unix_to_milliseconds,
)


version_7_0_0: Version = Version("7.0.0")
version_2_6_12: Version = Version("2.6.12")
version_6_0_0: Version = Version("6.0.0")
version_6_2_0: Version = Version("6.2.0")


class AbstractStringModel(RedisKey):
    async def _set(
        self,
        value: Any,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        keepttl: bool = False,
    ) -> StrBytesT:
        if nx and self.redisVersion < version_2_6_12:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support NX option. Required version: {version_2_6_12}",
            )
        if xx and self.redisVersion < version_2_6_12:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support XX option. Required version: {version_2_6_12}",
            )
        if ex and self.redisVersion < version_2_6_12:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support EX option. Required version: {version_2_6_12}",
            )
        if px and self.redisVersion < version_2_6_12:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PX option. Required version: {version_2_6_12}",
            )
        if get and self.redisVersion < version_6_2_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support GET option. Required version: {version_6_2_0}",
            )
        if exat and self.redisVersion < version_6_2_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support EXAT option. Required version: {version_6_2_0}",
            )
        if pxat and self.redisVersion < version_6_2_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support PXAT option. Required version: {version_6_2_0}",
            )
        if keepttl and self.redisVersion < version_6_0_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support KEEPTTL option. Required version: {version_6_0_0}",
            )

        if nx and xx:
            raise InvalidOptionsCombinationException(
                "NX and XX options both can not be True together"
            )

        if nx and get and self.redisVersion < version_7_0_0:
            raise InvalidOptionsCombinationException(
                f"Current version: {self.redisVersion} is not support NX and GET options to be used together. Required version: {version_7_0_0}",
            )

        pieces: List[FieldT] = [value]
        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")
        if get:
            pieces.append("GET")

        ttlCount = 0
        if ex is not None:
            ttlCount += 1
            pieces.append("EX")
            ex = timedetla_to_seconds(ex)
            pieces.append(ex)
        if px is not None:
            ttlCount += 1
            pieces.append("PX")
            px = timedetla_to_milliseconds(px)
            pieces.append(px)
        if exat is not None:
            ttlCount += 1
            pieces.append("EXAT")
            exat = unix_to_seconds(exat)
            pieces.append(exat)
        if pxat is not None:
            ttlCount += 1
            pieces.append("PXAT")
            pxat = unix_to_milliseconds(pxat)
            pieces.append(pxat)
        if keepttl:
            ttlCount += 1
            pieces.append("KEEPTTL")

        if ttlCount > 1:
            raise InvalidOptionsCombinationException(
                "EX, PX, EXAT, PXAT, KEEPTTL combination are not allow"
            )

        return await self.client.execute_command(  # pyright: ignore
            "SET", self.redisKey, *pieces
        )

    async def _get(self) -> StrBytesT:
        return await self.client.get(self.redisKey)  # pyright: ignore

    async def _getdel(self) -> StrBytesT:
        if self.redisVersion < version_6_2_0:
            raise OldRedisVersionException(
                f"Current version: {self.redisVersion} is not support GETDEL operation. Required version: {version_6_2_0}",
            )
        return await self.client.execute_command(  # pyright: ignore
            "GETDEL", self.redisKey
        )
