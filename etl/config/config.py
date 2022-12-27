from pydantic import BaseSettings


class Base(BaseSettings):
    db_name: str = 'movies_database'
    db_user: str = 'app'
    db_password: str = '123qwe'
    db_path: str = 'db.sqlite'
    host: str = '127.0.0.1'
    port: int = 5432
    # redis_name = 'localhost'
    es_host: str = 'localhost'
    es_port: int = 9200
    es_scheme: str = 'http'

    class Config:
        case_sensitive = False


settings = Base()
