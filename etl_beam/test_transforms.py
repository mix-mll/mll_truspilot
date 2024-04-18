import os

import apache_beam as beam
import fastavro
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from fastavro import reader
from transforms import RowToDict, WriteToAvro, dag_row_to_dict

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


class TestRowToDict:
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
            input_coll = pipeline | beam.Create([INPUT_DATA])
            actual_output = input_coll | beam.ParDo(RowToDict(HEADERS_TYPE))
            assert_that(actual_output, equal_to(OUTPUT_DICT))

    def test_dag_row_to_dict(self):
        with TestPipeline() as pipeline:
            input_coll = pipeline | beam.Create([INPUT_DATA])
            actual_output = dag_row_to_dict(input_coll, HEADERS_TYPE)
            assert_that(actual_output, equal_to(OUTPUT_GRUPED))


class TestWriteToAvro:
    FILENAME = "unit_test_writeavro"
    DESTINATION = "./test_files"
    CSV_FILENAME = f"{FILENAME}.csv"
    AVRO_FULL_FILENAME = f"{DESTINATION}/{FILENAME}.avro"

    def test_get_filename(self):
        write_to_avro = WriteToAvro(SCHEMA, self.DESTINATION, self.CSV_FILENAME)
        assert "path01/file02.avro" == write_to_avro.get_filename("path01", "file02.csv")

    def delete_file(self):
        if os.path.exists(self.AVRO_FULL_FILENAME):
            os.remove(self.AVRO_FULL_FILENAME)

    def read_avro(self):
        with open(self.AVRO_FULL_FILENAME, "rb") as fo:
            return [r for r in reader(fo)]

    def test_WriteToAvro(self):
        self.delete_file()

        with TestPipeline() as pipeline:
            input_coll = pipeline | "Create elements" >> beam.Create(OUTPUT_GRUPED)
            input_coll | "write-avro" >> beam.ParDo(WriteToAvro(SCHEMA, self.DESTINATION, self.CSV_FILENAME))

        assert self.read_avro() == OUTPUT_DICT

        self.delete_file()
