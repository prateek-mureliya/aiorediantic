import pytest
from pydantic.error_wrappers import ValidationError

from aiorediantic import RedisConfig


@pytest.mark.parametrize("wrong_version", ["sggsh", "1.a.b", "x.y.z"])
def test_shouldRaiseValidationError_whenWrongVersionPass(wrong_version: str) -> None:
    # Act
    with pytest.raises(ValidationError) as ex_info:
        RedisConfig(redis_version=wrong_version)

    # Assert
    excepted: str = "Invalid version: '" + wrong_version + "'"
    exception: ValidationError = ex_info.value
    assert ex_info.type == ValidationError
    for err in exception.errors():
        assert err["loc"][0] == "redis_version"
        assert err["msg"] == excepted


@pytest.mark.parametrize(
    "correct_version",
    [
        "1c2",
        "1.7",
        "2.10b2",
        "1.8.1",
        "2.0.0a1",
        "3.10.0b1",
        "111111111111.22222222222.3333333333a90",
    ],
)
def test_shouldPass_whenCorrectVersionPass(correct_version: str) -> None:
    # Act
    config = RedisConfig(redis_version=correct_version)

    # Assert
    assert config.redis_version == correct_version
