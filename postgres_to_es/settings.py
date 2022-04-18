from pydantic import BaseSettings, Field
from enum import Enum


class PosgressSettings(BaseSettings):
    dbname: str = Field(..., env='pg_dbname')
    user: str = Field(..., env='pg_user')
    password: str = Field(..., env='pg_password')
    host: str = Field(..., env='pg_host')
    port: int = Field(..., env='pg_port')


class ElkSettings(BaseSettings):
    elk_host: str = Field(..., env='elk_host')
    elk_port: str = Field(..., env='elk_port')
    elk_index: str = Field(..., env='elk_index')


class IndexsEnum(Enum):
    movies = "movies"
    genres = "genres"
    persons = "persons"
