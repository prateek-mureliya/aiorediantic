from typing import Any, Optional


from aiorediantic.types import StrBytesT, AbsExpiryT, ExpiryT, OptionalStr
from aiorediantic.utils import str_if_byte
from .abstract_string import AbstractStringModel


class StringModel(AbstractStringModel):
    async def set(
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
    ) -> OptionalStr:
        status: StrBytesT = await super().set(
            value=value,
            nx=nx,
            xx=xx,
            get=get,
            ex=ex,
            px=px,
            exat=exat,
            pxat=pxat,
            keepttl=keepttl,
        )

        return str_if_byte(status)
