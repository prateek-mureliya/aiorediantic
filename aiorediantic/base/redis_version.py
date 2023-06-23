from pydantic import BaseModel


class RedisVersion(BaseModel):
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: "RedisVersion"):
        if (
            (self.major < other.major)
            or (self.major == other.major and self.minor < other.minor)
            or (
                self.major == other.major
                and self.minor == other.minor
                and self.patch < other.patch
            )
        ):
            return True
        else:
            return False
