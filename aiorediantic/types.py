from typing import Union
import datetime

EncodedT = Union[bytes, memoryview]
DecodedT = Union[str, int, float]
FieldT = Union[EncodedT, DecodedT]
AbsExpiryT = Union[int, datetime.datetime]
ExpiryT = Union[int, datetime.timedelta]
StrBytesT = Union[str, bytes, None]
OptionalStr = Union[str, None]
