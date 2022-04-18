"""Class to work with postgress."""
import logging
from contextlib import closing
from typing import Any

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from decorators import backoff
from exceptions import RetryExceptionError
from settings import PosgressSettings

logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh = logging.FileHandler(filename="/var/log/elk_service/exceptions.log")
fh.setFormatter(formatter)
logger.addHandler(fh)


class DBConnector:
    """Class to work with postgress db."""

    def __init__(self, config: PosgressSettings) -> None:
        """Init db connect.

        Args:
            config: PosgressSettings connection configration.
        """
        self.config = config.dict()

    @backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def get_connection(self) -> _connection:
        """Get connection to Database.

        Returns:
            _connection

        Raises:
            RetryExceptionError: if OperationalError triggered.
        """
        try:
            connect = psycopg2.connect(**self.config, cursor_factory=DictCursor)
        except psycopg2.OperationalError:
            raise RetryExceptionError("Postgress database is not available, retrying...")
        return connect

    @backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def load_data(self, sql: str) -> list[Any]:
        """Execute sql query.

        Args:
            sql: str sql query to execute.

        Returns:
            list: sql_result

        Raises:
            RetryExceptionError: if OperationalError triggered.
        """
        with closing(self.get_connection()) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except psycopg2.OperationalError:
                raise RetryExceptionError("Postgress database is not available, retrying...")
            sql_result = cursor.fetchall()
            cursor.close()
        return sql_result
