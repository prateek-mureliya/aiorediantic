from typing import Any, Optional, Type
from pydantic import BaseModel, create_model
from packaging.version import Version
import aioredis


from aiorediantic.config import RedisConfig
from .redis_client import RedisClient


class RedisModel(BaseModel):
    _redisKey: Optional[str] = None
    redisClient: RedisClient
    keyFormat: str
    vars: BaseModel = BaseModel()
    redisVersion: Version = Version("1.0.0")

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
        dynamicModel: Type[BaseModel] = create_model("vars", **kwargs)  # type: ignore
        self.vars = dynamicModel()

        self.redisVersion = Version(self.config.redis_version)
        return self
