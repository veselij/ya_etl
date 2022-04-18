"""Manage state of program."""
import json
from abc import ABC, abstractmethod
from typing import Any


class BaseStorage(ABC):

    @abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as f:
                state = json.load(f)
        except FileNotFoundError:
            state = {}
        return state


class State:

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.data = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.storage.save_state(self.data)

    def get_state(self, key: str) -> Any:
        return self.data.get(key)
