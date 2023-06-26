"""Module containing the main config classes"""
from typing import Optional, Union, Mapping
from enum import Enum
from packaging.version import Version

from pydantic import BaseModel, validator


class RedisScheme(str, Enum):
    redis = "redis://"
    rediss = "rediss://"


class RedisConfig(BaseModel):
    """A config object for connecting to redis"""

    redis_version: str
    scheme: RedisScheme = RedisScheme.redis
    host: str = "localhost"
    port: int = 6379
    db: Union[str, int] = 0
    password: Optional[str] = None
    username: Optional[str] = None
    client_name: Optional[str] = None
    socket_type: int = 0
    socket_timeout: Optional[float] = None
    socket_connect_timeout: Optional[float] = None
    socket_keepalive: Optional[bool] = None
    socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None
    socket_read_size: int = 65536
    encoding: str = "utf-8"
    encoding_errors: str = "strict"
    decode_responses: bool = False
    retry_on_timeout: bool = False
    max_connections: Optional[int] = None
    health_check_interval: int = 0
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_cert_reqs: str = "required"
    ssl_ca_certs: Optional[str] = None
    ssl_check_hostname: bool = False

    @validator("redis_version")
    def redis_version_must_be_validate(cls, version: str) -> str:
        Version(version)
        return version

    class Config:
        """Pydantic schema config"""

        orm_mode = True
