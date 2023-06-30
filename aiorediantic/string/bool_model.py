from typing import Optional


from aiorediantic import InvalidArgumentTypeException, UnexpectedReturnTypeException
from aiorediantic.types import StrBytesT, AbsExpiryT, ExpiryT, BoolReturn, StrReturn
from aiorediantic.utils import str_if_byte
from .abstract_string import AbstractStringModel


class BoolModel(AbstractStringModel):
    def _parse_res(self, value: StrBytesT, get: bool = True) -> BoolReturn:
        res: StrReturn = str_if_byte(value)
        if res is False:
            return 0

        if not get and res == "OK":
            return 1

        if res in ["true", "false"]:
            return res == "true"
        else:
            raise UnexpectedReturnTypeException(
                f"BoolModel expect BOOL return type but get value {res}"
            )

    async def set(
        self,
        value: bool,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: Optional[ExpiryT] = None,
        px: Optional[ExpiryT] = None,
        exat: Optional[AbsExpiryT] = None,
        pxat: Optional[AbsExpiryT] = None,
        keepttl: bool = False,
    ) -> BoolReturn:
        """
        @Available since: 1.0.0
        Set key to hold the bool value.

        Return
            1 if SET was executed correctly.
            0 if the SET operation was not performed because the user specified the NX or XX option but the condition was not met.
            Old bool value stored at key.If the command is issued with the GET option.
            0 if the key did not exist.

        The SET command supports a set of options that modify its behavior:
            EX seconds -- Set the specified expire time, in seconds.
            PX milliseconds -- Set the specified expire time, in milliseconds.
            EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
            PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
            NX -- Only set the key if it does not already exist.
            XX -- Only set the key if it already exists.
            KEEPTTL -- Retain the time to live associated with the key.
            GET -- Return the old bool stored at key, or nil if key did not exist.

        History
            Starting with Redis version 2.6.12: Added the EX, PX, NX and XX options.
            Starting with Redis version 6.0.0: Added the KEEPTTL option.
            Starting with Redis version 6.2.0: Added the GET, EXAT and PXAT option.
            Starting with Redis version 7.0.0: Allowed the NX and GET options to be used together.
        """
        if type(value) != bool:  # pyright: ignore
            raise InvalidArgumentTypeException("BoolModel allow to set only BOOL value")

        status: StrBytesT = await super()._set(
            value="true" if value else "false",
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

    async def get(self) -> BoolReturn:
        """
        @Available since: 1.0.0
        Get the bool value of key.

        Return
            bool value of key
            0 when key does not exist.
        """
        status: StrBytesT = await super()._get()
        return self._parse_res(status, get=True)

    async def getdel(self) -> BoolReturn:
        """
        @Available since: 6.2.0
        Get the bool value of key and delete the key.

        Return
            bool value of key
            0 when key does not exist.
        """
        status: StrBytesT = await super()._getdel()
        return self._parse_res(status, get=True)
