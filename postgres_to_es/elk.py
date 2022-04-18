"""Logic to load data to Elasticsearch."""
import json
import logging
import os
from dataclasses import asdict
from typing import Generator

from elasticsearch import ConnectionError, Elasticsearch
from elasticsearch.helpers import bulk

from decorators import backoff
from exceptions import RetryExceptionError
from settings import ElkSettings
from transformator import BaseDoc

logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh = logging.FileHandler(filename="/var/log/elk_service/exceptions.log")
fh.setFormatter(formatter)
logger.addHandler(fh)


class ELKLoader:
    """Class loader data to Elasticsearch."""

    def __init__(self, config: ElkSettings) -> None:
        """Init ELKLoader.

        Args:
            config: ElkSettings connection details to Elasticsearch and index config

        """
        self.config = config
        self.create_indexs()

    @backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def create_index(self, index_file: str) -> None:
        """Create required index in Elasticsearch.

        Args:
            index_file: str file with elasticsearch index structure

        Raises:
            RetryExceptionError: if ConnectionError triggered
        """
        client = self.get_client()
        with open(index_file, "r") as fl:
            index_description = json.load(fl)
        try:
            client.options(ignore_status=400).indices.create(
                index=os.path.basename(index_file).split(".")[0],
                **index_description,
            )
        except ConnectionError:
            raise RetryExceptionError("Elasticsearch is not available, retrying...")
        finally:
            client.close()

    def create_indexs(self) -> None:
        """Create index in elasticsearch from index files in index folder."""
        for index_file in os.listdir(self.config.elk_index):
            self.create_index(os.path.join(self.config.elk_index, index_file))

    @backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def load(self, data_to_load: dict) -> None:
        """Load data to Elasticsearch index.

        Args:
            data_to_load: dict data to load in Elasticsearch index.

        Raises:
            RetryExceptionError: if ConnectionError triggered.
        """
        client = self.get_client()
        try:
            bulk(client=client, actions=self.generate_doc(data_to_load))
        except ConnectionError:
            raise RetryExceptionError("Elasticsearch is not available, retrying...")
        finally:
            client.close()

    def generate_doc(self, batch: dict[str, BaseDoc]) -> Generator[dict, None, None]:
        """Generate items for bulk elasticsearch loader.

        Args:
            batch: dict dictionary to convert from to elasticsearch format.

        Yields:
            dict: items in elasticsearch format.
        """
        for key, row in batch.items():
            yield {
                "_index": row.index_name,
                "_id": key,
                "_source": asdict(row),
            }

    def get_client(self) -> Elasticsearch:
        """Get elasticsearch client.

        Returns:
            Elasticsearch: client to elasticsearch
        """
        return Elasticsearch(
            hosts="http://{host}:{port}".format(host=self.config.elk_host, port=self.config.elk_port),
            max_retries=0,
        )
