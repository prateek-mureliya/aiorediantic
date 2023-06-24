from typing import Any, Optional, Type
from pydantic import BaseModel, create_model
import aioredis


from aiorediantic.config import RedisConfig
from .redis_version import RedisVersion
from .redis_client import RedisClient


class RedisModel(BaseModel):
    _redisKey: Optional[str] = None
    redisClient: RedisClient
    keyFormat: str
    vars: BaseModel = BaseModel()
    redisVersion: RedisVersion = RedisVersion(major=1, minor=0, patch=0)

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    @property
    def client(self) -> aioredis.Redis:
        return self.redisClient.client

    @property
    def config(self) -> RedisConfig:
        return self.redisClient.config

    @property
    def redisKey(self) -> str:
        if not self._redisKey:
            self._redisKey = self.keyFormat.format(**self.vars.dict())
        return self._redisKey

    def __call__(self, **kwargs: Any):
        dynamicModel: Type[BaseModel] = create_model("vars", **kwargs)
        self.vars = dynamicModel()

        parts: list[str] = self.config.redis_version.split(".")
        self.redisVersion = RedisVersion(
            major=int(parts[0]), minor=int(parts[1]), patch=int(parts[2])
        )
        return self
