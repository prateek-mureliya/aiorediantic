from typing import Any, Optional, Union, List
from aioredis.client import ExpiryT, FieldT, AbsExpiryT


from .base.redis_key import RedisKey
from .exception import InvalidOptionsCombinationException
from .utils import (
    timedetla_to_seconds,
    timedetla_to_milliseconds,
    unix_to_seconds,
    unix_to_milliseconds,
)


class AbstractStringModel(RedisKey):
    async def set(
        self,
        value: Any,
        nx: bool = False,
        xx: bool = False,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        keepttl: bool = False,
    ) -> Union[str, bool, None]:
        if nx and xx:
            raise InvalidOptionsCombinationException(
                "NX and XX both can not be True together"
            )

        pieces: List[FieldT] = [value]
        if nx:
            pieces.append("NX")
        if xx:
            pieces.append("XX")

        if ex is not None:
            pieces.append("EX")
            ex = timedetla_to_seconds(ex)
            pieces.append(ex)
        if px is not None:
            pieces.append("PX")
            px = timedetla_to_milliseconds(px)
            pieces.append(px)
        if exat is not None:
            pieces.append("EXAT")
            exat = unix_to_seconds(exat)
            pieces.append(exat)
        if pxat is not None:
            pieces.append("PXAT")
            pxat = unix_to_milliseconds(pxat)
            pieces.append(pxat)
        if keepttl:
            pieces.append("KEEPTTL")

        return await self.client.execute_command(  # pyright: ignore
            "SET", self.redisKey, *pieces
        )

    async def get(self) -> Any:
        return await self.client.get(self.redisKey)  # pyright: ignore

    async def getset(self, value: Any) -> Any:
        return await self.client.get(self.redisKey)  # pyright: ignore


class StringModel(AbstractStringModel):
    pass
