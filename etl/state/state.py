import abc
from typing import Any, Optional
import json
import os
from redis import Redis


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
    pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
    pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis, dict_name: Optional[str] = None):
        self.redis_adapter = redis_adapter
        self.dict_name = dict_name

    def save_state(self, state: dict) -> None:

        self.redis_adapter.hset(self.dict_name, mapping=state)

    def retrieve_state(self) -> dict:
        byte_dict = self.redis_adapter.hgetall(self.dict_name)
        state_dict = {key.decode(): val.decode() for key, val in byte_dict.items()}
        return state_dict


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        self.data = dict()

    def save_state(self, state: dict) -> None:
        if self.file_path:
            with open(self.file_path) as read_file:
                self.data = json.load(read_file)
                for key in state:
                    if self.data[key] != state[key]:
                        self.data[key] = state[key]
            with open(self.file_path, 'w') as write_file:
                json.dump(self.data, write_file, indent=2, default=str)
        else:
            self.data = json.dumps(state)

    def retrieve_state(self) -> dict:
        if self.file_path:
            try:
                if os.stat(self.file_path).st_size > 0:
                    with open(self.file_path) as read_file:
                        self.data = json.load(read_file)
                        return self.data
                else:
                    return self.data
            except OSError:
                return self.data
        else:
            return self.data


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        result = self.storage.retrieve_state().get(key, None)
        if result:
            return result
        else:
            return None
