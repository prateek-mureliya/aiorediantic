import pytest_asyncio


from aiorediantic import RedisConfig, RedisClient

conf = dict(host="127.0.0.1", decode_responses=True)
high_version = "7.0.11"


@pytest_asyncio.fixture()  # pyright: ignore
async def redis_client():
    redis = RedisClient(config=RedisConfig.construct(redis_version=high_version, **conf))  # type: ignore
    yield redis
    await redis.client.close()


@pytest_asyncio.fixture()  # pyright: ignore
async def redis_client_2_6_0():
    redis = RedisClient(config=RedisConfig.construct(redis_version="2.6.0", **conf))  # type: ignore
    yield redis
    await redis.client.close()


@pytest_asyncio.fixture()  # pyright: ignore
async def redis_client_1_2_0():
    redis = RedisClient(config=RedisConfig.construct(redis_version="1.2.0", **conf))  # type: ignore
    yield redis
    await redis.client.close()
