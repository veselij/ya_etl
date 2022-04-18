"""Loader."""
from time import sleep

from db import DBConnector
from elk import ELKLoader
from producer import BaseProducer, schemas
from settings import ElkSettings, PosgressSettings
from state import JsonFileStorage, State
from transformator import transform_lists_to_dc


def main():
    connector = DBConnector(PosgressSettings())

    elk_loader = ELKLoader(ElkSettings())

    state = State(JsonFileStorage('/var/log/elk_service/state.json'))

    producers = []

    for key, schema_class in schemas.items():
        schema = schema_class(tracked_id=key, related_id="{0}_related".format(key))
        producers.append(BaseProducer(connector, state, schema))

    while True:
        for producer in producers:
            for pg_data in producer.get_results():
                elk_loader.load(transform_lists_to_dc(pg_data))
        sleep(10)


if __name__ == "__main__":
    main()
