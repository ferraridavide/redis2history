from dataclasses import dataclass
from typing import Optional


@dataclass
class RedisSettings:
    host: str
    port: int
    tls: bool
    cluster: bool
    lock: Optional[str] = None
    lockTimeout: Optional[int] = 10

@dataclass
class MongoSettings:
    host: str
    port: int
    tls: bool

@dataclass
class Transforms:
    id: str
    script: str
    args: Optional[dict[str, str]] = None
    workers: Optional[int] = 1

@dataclass
class PipelineModel:
    id: str
    stages: list[Transforms]
    source: Optional[str] = None 
    bulkSource: Optional[bool] = False
    parallel: Optional[bool] = True

@dataclass
class Config:
    redisSettings: RedisSettings
    mongoSettings: MongoSettings
    globalArgs: dict
    pipelines: list[PipelineModel]

