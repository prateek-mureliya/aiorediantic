from typing import Any, Optional
from pydantic import BaseModel
import aioredis


from ..config import RedisConfig, RedisScheme


class RedisClient(BaseModel):
    _client: Optional[aioredis.Redis] = None
    config: RedisConfig

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    @property
    def client(self) -> aioredis.Redis:
        if not self._client:
            kwargs: dict[str, Any] = {
                "host": self.config.host,
                "port": self.config.port,
                "db": self.config.db,
                "username": self.config.username,
                "password": self.config.password,
                "client_name": self.config.client_name,
                "socket_type": self.config.socket_type,
                "socket_timeout": self.config.socket_timeout,
                "socket_connect_timeout": self.config.socket_connect_timeout,
                "socket_keepalive": self.config.socket_keepalive,
                "socket_keepalive_options": self.config.socket_keepalive_options,
                "socket_read_size": self.config.socket_read_size,
                "encoding": self.config.encoding,
                "encoding_errors": self.config.encoding_errors,
                "decode_responses": self.config.decode_responses,
                "retry_on_timeout": self.config.retry_on_timeout,
                "max_connections": self.config.max_connections,
                "health_check_interval": self.config.health_check_interval,
            }
            if self.config.scheme == RedisScheme.rediss:
                kwargs.update(
                    {
                        "ssl_keyfile": self.config.ssl_keyfile,
                        "ssl_certfile": self.config.ssl_certfile,
                        "ssl_cert_reqs": self.config.ssl_cert_reqs,
                        "ssl_ca_certs": self.config.ssl_ca_certs,
                        "ssl_check_hostname": self.config.ssl_check_hostname,
                    }
                )
            self._client = aioredis.from_url(  # pyright: ignore
                self.config.scheme, **kwargs
            )
        return self._client
