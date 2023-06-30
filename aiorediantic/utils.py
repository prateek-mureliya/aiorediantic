import time as mod_time
import datetime


from aiorediantic.types import StrBytesT, AbsExpiryT, ExpiryT, StrReturn


def timedetla_to_seconds(seconds: ExpiryT) -> int:
    if isinstance(seconds, datetime.timedelta):
        seconds = int(seconds.total_seconds())
    return seconds


def timedetla_to_milliseconds(milliseconds: ExpiryT) -> int:
    if isinstance(milliseconds, datetime.timedelta):
        milliseconds = int(milliseconds.total_seconds() * 1000)
    return milliseconds


def unix_to_seconds(epoch_in_seconds: AbsExpiryT) -> int:
    if isinstance(epoch_in_seconds, datetime.datetime):
        epoch_in_seconds = int(mod_time.mktime(epoch_in_seconds.timetuple()))
    return epoch_in_seconds


def unix_to_milliseconds(epoch_in_milliseconds: AbsExpiryT) -> int:
    if isinstance(epoch_in_milliseconds, datetime.datetime):
        ms = int(epoch_in_milliseconds.microsecond / 1000)
        epoch_in_milliseconds = (
            int(mod_time.mktime(epoch_in_milliseconds.timetuple())) * 1000 + ms
        )
    return epoch_in_milliseconds


def str_if_byte(value: StrBytesT) -> StrReturn:
    if value:
        return (
            value.decode("utf-8", errors="replace")
            if isinstance(value, bytes)
            else value
        )
    else:
        return False
