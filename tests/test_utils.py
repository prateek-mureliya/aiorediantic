import pytest
import time as mod_time
from datetime import timedelta, datetime


from aiorediantic.types import ExpiryT, AbsExpiryT, StrBytesT, StrReturn
from aiorediantic.utils import (
    timedetla_to_seconds,
    timedetla_to_milliseconds,
    unix_to_seconds,
    unix_to_milliseconds,
    str_if_byte,
)


@pytest.mark.parametrize(
    "time",
    [
        120,
        pytest.param(timedelta(hours=1), id="timedelta(hours=1)"),
        pytest.param(timedelta(minutes=3), id="timedelta(minutes=3)"),
        pytest.param(timedelta(seconds=45), id="timedelta(seconds=45)"),
    ],
)
def testTimedetla_to_seconds_shouldPass_whenIntAndTimedeltaPassAsTime(
    time: ExpiryT,
) -> None:
    # Act
    actual: int = timedetla_to_seconds(seconds=time)

    # Assert
    if isinstance(time, timedelta):
        excepted = int(time.total_seconds())
    else:
        excepted: int = time

    assert type(actual) == int
    assert actual == excepted


@pytest.mark.parametrize(
    "time",
    [
        120000,
        pytest.param(timedelta(hours=1), id="timedelta(hours=1)"),
        pytest.param(timedelta(minutes=3), id="timedelta(minutes=3)"),
        pytest.param(timedelta(seconds=45), id="timedelta(seconds=45)"),
    ],
)
def testTimedetla_to_milliseconds_shouldPass_whenIntAndTimedeltaPassAsTime(
    time: ExpiryT,
) -> None:
    # Act
    actual: int = timedetla_to_milliseconds(milliseconds=time)

    # Assert
    if isinstance(time, timedelta):
        excepted = int(time.total_seconds() * 1000)
    else:
        excepted: int = time

    assert type(actual) == int
    assert actual == excepted


@pytest.mark.parametrize(
    "time",
    [
        1687871169,
        pytest.param(
            datetime(year=2012, month=6, day=12, hour=1, minute=20, second=45),
            id="datetime('2012-06-12 01:20:45')",
        ),
    ],
)
def testUnix_to_seconds_shouldPass_whenIntAndDatetimePassAsTime(
    time: AbsExpiryT,
) -> None:
    # Act
    actual: int = unix_to_seconds(epoch_in_seconds=time)

    # Assert
    if isinstance(time, datetime):
        excepted = int(mod_time.mktime(time.timetuple()))
    else:
        excepted: int = time

    assert type(actual) == int
    assert actual == excepted


@pytest.mark.parametrize(
    "time",
    [
        1687871169876,
        pytest.param(
            datetime(
                year=2012,
                month=6,
                day=12,
                hour=1,
                minute=20,
                second=45,
                microsecond=876,
            ),
            id="datetime('2012-06-12 01:20:45.876')",
        ),
    ],
)
def testUnix_to_milliseconds_shouldPass_whenIntAndDatetimePassAsTime(
    time: AbsExpiryT,
) -> None:
    # Act
    actual: int = unix_to_milliseconds(epoch_in_milliseconds=time)

    # Assert
    if isinstance(time, datetime):
        ms = int(time.microsecond / 1000)
        excepted = int(mod_time.mktime(time.timetuple())) * 1000 + ms
    else:
        excepted: int = time

    assert type(actual) == int
    assert actual == excepted


@pytest.mark.parametrize(
    "value",
    [
        None,
        "OK",
        "abc",
        b"bytes",
    ],
)
def testStr_if_byte_shouldPass(
    value: StrBytesT,
) -> None:
    # Act
    actual: StrReturn = str_if_byte(value)

    # Assert
    if isinstance(value, bytes):
        excepted: StrReturn = value.decode("utf-8", errors="replace")
    elif value is None:
        excepted: StrReturn = False
    else:
        excepted: StrReturn = value

    if value:
        assert type(actual) == str
    else:
        assert type(actual) == bool

    assert actual == excepted
