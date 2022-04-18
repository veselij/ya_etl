"""Buisness logic to collect data from database."""
from dataclasses import dataclass
from typing import Callable, Generator, Union

from db import DBConnector
from settings import IndexsEnum
from sql import (
    SQL_GENRE_GET_FILM_IDs,
    SQL_GENRE_GET_TRACKED_IDs,
    SQL_GET_FILMs,
    SQL_GET_GENREs,
    SQL_GET_PERSONs,
    SQL_MOVIE_GET_TRACKED_IDs,
    SQL_PERSON_GET_FILM_IDs,
    SQL_PERSON_GET_TRACKED_IDs,
    SQL_STANDALONE_GENRE_GET_TRACKED_IDs,
    SQL_STANDALONE_PERSON_GET_TRACKED_IDs,
)
from state import State


@dataclass(frozen=True)
class Schema:
    """Dataclass to store SQL queries text templates to database."""

    tracked_id: str
    related_id: str
    index_name: str
    sql_get_tracked_ids: str
    sql_get_data: str
    sql_get_ids: str = ""


@dataclass(frozen=True)
class PersonShema(Schema):
    """Dataclass to store SQL queries text templates to database for scanning Person table for index movies."""

    index_name: str = IndexsEnum.movies.value
    sql_get_tracked_ids: str = SQL_PERSON_GET_TRACKED_IDs
    sql_get_data: str = SQL_GET_FILMs
    sql_get_ids: str = SQL_PERSON_GET_FILM_IDs


@dataclass(frozen=True)
class GenreShema(Schema):
    """Dataclass to store SQL queries text templates to database for scanning Genres table for index movies."""

    index_name: str = IndexsEnum.movies.value
    sql_get_tracked_ids: str = SQL_GENRE_GET_TRACKED_IDs
    sql_get_data: str = SQL_GET_FILMs
    sql_get_ids: str = SQL_GENRE_GET_FILM_IDs


@dataclass(frozen=True)
class MovieShema(Schema):
    """Dataclass to store SQL queries text templates to database for scanning Movie table for index movies."""

    index_name: str = IndexsEnum.movies.value
    sql_get_tracked_ids: str = SQL_MOVIE_GET_TRACKED_IDs
    sql_get_data: str = SQL_GET_FILMs


@dataclass(frozen=True)
class GenreIndexSchema(Schema):
    """Dataclass to store SQL queries text templates to database for scanning Genre table for index genre."""

    index_name: str = IndexsEnum.genres.value
    sql_get_tracked_ids: str = SQL_STANDALONE_GENRE_GET_TRACKED_IDs
    sql_get_data: str = SQL_GET_GENREs


@dataclass(frozen=True)
class PersonIndexSchema(Schema):
    """Dataclass to store SQL queries text templates to database for scanning Person table for index persons."""

    index_name: str = IndexsEnum.persons.value
    sql_get_tracked_ids: str = SQL_STANDALONE_PERSON_GET_TRACKED_IDs
    sql_get_data: str = SQL_GET_PERSONs


schemas = {
    'person': PersonShema,
    'genre': GenreShema,
    'movie': MovieShema,
    'genre_index': GenreIndexSchema,
    'person_index': PersonIndexSchema,
}


class BaseProducer:
    """Buisness logic to get list of films with all required for ELS details."""

    def __init__(self, connector: DBConnector, state: State, schema: Schema) -> None:
        """Init of Base producer.

        Args:
            connector: DBConnector class to work with database
            state: State class to handle and store state changes
            schema: Dataclass with all required SQL templates
        """
        self.connector = connector
        self.state = state
        self.local_state = {}
        self.schema = schema

    def save_current_ids(self, key: str) -> str:
        """Save last processed time in persistance storage.

        Args:
            key: str name of saved metric

        Returns:
            str: last processed time
        """
        last_processed_time = self.get_last_id_time(key)
        self.state.set_state(key, last_processed_time)
        return last_processed_time

    def get_films(
        self,
        func_name: Callable[[list], Union[str, tuple[str, list]]],
        sql: str,
        key: str,
        **kwargs,
    ) -> Generator[Union[str, tuple[str, list]], None, None]:
        """Get films.

        Args:
            func_name: Callable function apply
            sql: sql query
            key: str name of metric
            **kwargs: Arbitrary keyword arguments.

        Yields:
            Union[str, tuple[str, list]]: related ids or films list
        """
        while True:
            data_from_db = self.connector.load_data(sql.format(last_tracked=self.save_current_ids(key), **kwargs))
            if not data_from_db:
                break
            self.local_state[key] = data_from_db[-1][1].strftime("%Y-%m-%d %H:%M:%S.%f")
            yield func_name(data_from_db)

    def load_films(self, film_ids: list) -> tuple[str, list]:
        """Load films for output results.

        Args:
            film_ids: str

        Returns:
            tupe[str, list]: films
        """
        return (
            self.schema.index_name,
            self.connector.load_data(self.schema.sql_get_data.format(film_ids=self.convert_ids(film_ids))),
        )

    def get_results(self) -> Generator[Union[str, tuple[str, list]], None, None]:
        """Get films.

        Yields:
            Union[str, tuple[str, list]] films
        """
        for tracked_ids in self.get_films(self.convert_ids, self.schema.sql_get_tracked_ids, self.schema.tracked_id):
            if self.schema.sql_get_ids:
                self.state.set_state(self.schema.related_id, '2000-01-01')
                self.local_state[self.schema.related_id] = '2000-01-01'
                yield from self.get_films(
                    self.load_films,
                    self.schema.sql_get_ids,
                    self.schema.related_id,
                    tracked_ids=tracked_ids,
                )
            else:
                yield (
                    self.schema.index_name,
                    self.connector.load_data(self.schema.sql_get_data.format(film_ids=tracked_ids)),
                )

    def get_last_id_time(self, key: str) -> str:
        """Get time of last processed data.

        Args:
            key: str name of the metric.

        Returns:
            str: last proccessed time
        """
        local_state = self.local_state.get(key)
        last_tracked_id_time = self.state.get_state(key) if local_state is None else local_state
        return last_tracked_id_time if last_tracked_id_time is not None else '2000-01-01'

    def convert_ids(self, data_from_db: list[tuple]) -> str:
        """Convert list items to string.

        Args:
            data_from_db: list[tuple]

        Returns:
            str: joined with commas string.
        """
        return ",".join(["'{0}'".format(row[0]) for row in data_from_db])
