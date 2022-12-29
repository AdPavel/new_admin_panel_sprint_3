from pydantic import BaseSettings


class Base(BaseSettings):
    # Postgres section
    db_name: str = 'movies_database'
    db_user: str = 'app'
    db_password: str = '123qwe'
    port: int = 5432
    host: str = 'db'
    # Для локального тестирования
    # host: str = '127.0.0.1'

    # Elastic section
    es_port: int = 9200
    es_scheme: str = 'http'
    es_host: str = 'elastic'
    # Для локального тестирования
    # es_host: str = 'localhost'

    # Backoff section
    border_time: int = 30

    # ETL section
    iteration_pause: int = 30
    storage_file: str = 'state_store.json'

    class Config:
        case_sensitive = False


settings = Base()
