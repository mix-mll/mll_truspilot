import apache_beam as beam
import fastavro
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from transforms import RowToDict, dag_row_to_dict

SCHEMA = {
    "namespace": "orders.avro",
    "name": "orders_avro_m2",
    "type": "record",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int"},
    ],
}

HEADERS_TYPE = {f["name"]: f["type"] for f in SCHEMA["fields"]}
PARSED_SCHEMA = fastavro.schema.parse_schema(SCHEMA)


class TestRowToDict:
    INPUT_DATA = [
        "name;id",
        "a;1",
        "b;2",
    ]

    OUTPUT_DICT = [
        {"name": "a", "id": 1},
        {"name": "b", "id": 2},
    ]

    OUTPUT_GRUPED = [(1, OUTPUT_DICT)]

    def test_tuple_str_to_dict(self):
        # defaul separtor
        row_to_dict = RowToDict(HEADERS_TYPE)
        assert {"name": "a", "id": 1} == row_to_dict.tuple_str_to_dict("a;1")

        # ignore headers
        assert row_to_dict.tuple_str_to_dict("name;id") is None

        # custom separtor
        row_to_dict = RowToDict(HEADERS_TYPE, separtor="|")
        assert {"name": "b", "id": 2} == row_to_dict.tuple_str_to_dict("b|2")

    def test_RowToDict(self):
        with TestPipeline() as pipeline:
            input_coll = pipeline | beam.Create([self.INPUT_DATA])
            actual_output = input_coll | beam.ParDo(RowToDict(HEADERS_TYPE))
            assert_that(actual_output, equal_to(self.OUTPUT_DICT))

    def test_dag_row_to_dict(self):
        with TestPipeline() as pipeline:
            input_coll = pipeline | beam.Create([self.INPUT_DATA])
            actual_output = dag_row_to_dict(input_coll, HEADERS_TYPE)
            assert_that(actual_output, equal_to(self.OUTPUT_GRUPED))
