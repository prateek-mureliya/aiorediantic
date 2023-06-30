from typing import Optional


from aiorediantic import InvalidArgumentTypeException, UnexpectedReturnTypeException
from aiorediantic.types import StrBytesT, AbsExpiryT, ExpiryT, FloatReturn, StrReturn
from aiorediantic.utils import str_if_byte
from .abstract_string import AbstractStringModel


class FloatModel(AbstractStringModel):
    def _parse_res(self, value: StrBytesT, get: bool = True) -> FloatReturn:
        res: StrReturn = str_if_byte(value)
        try:
            if type(res) == bool:
                return res

            if not get and res == "OK":
                return True

            return float(res)
        except:
            raise UnexpectedReturnTypeException(
                f"FloatModel expect FLOAT return type but get value {res}"
            )

    async def set(
        self,
        value: float,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        keepttl: bool = False,
    ) -> FloatReturn:
        """
        @Available since: 1.0.0
        Set key to hold the float value.

        Return
            True if SET was executed correctly.
            False if the SET operation was not performed because the user specified the NX or XX option but the condition was not met.
            Old float value stored at key.If the command is issued with the GET option.
            False if the key did not exist.

        The SET command supports a set of options that modify its behavior:
            EX seconds -- Set the specified expire time, in seconds.
            PX milliseconds -- Set the specified expire time, in milliseconds.
            EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
            PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
            NX -- Only set the key if it does not already exist.
            XX -- Only set the key if it already exists.
            KEEPTTL -- Retain the time to live associated with the key.
            GET -- Return the old float stored at key, or nil if key did not exist.

        History
            Starting with Redis version 2.6.12: Added the EX, PX, NX and XX options.
            Starting with Redis version 6.0.0: Added the KEEPTTL option.
            Starting with Redis version 6.2.0: Added the GET, EXAT and PXAT option.
            Starting with Redis version 7.0.0: Allowed the NX and GET options to be used together.
        """
        if type(value) != float:
            raise InvalidArgumentTypeException(
                "FloatModel allow to set only FLOAT value"
            )

        status: StrBytesT = await super()._set(
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

        return self._parse_res(status, get=get)

    async def get(self) -> FloatReturn:
        """
        @Available since: 1.0.0
        Get the float value of key.

        Return
            float value of key
            False when key does not exist.
        """
        status: StrBytesT = await super()._get()
        return self._parse_res(status, get=True)

    async def getdel(self) -> FloatReturn:
        """
        @Available since: 6.2.0
        Get the float value of key and delete the key.

        Return
            float value of key
            False when key does not exist.
        """
        status: StrBytesT = await super()._getdel()
        return self._parse_res(status, get=True)
