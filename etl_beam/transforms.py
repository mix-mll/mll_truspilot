from io import StringIO

import apache_beam as beam
import fastavro
import pandas as pd
from apache_beam import DoFn
from apache_beam.io.filesystems import FileSystems


class WriteToAvro(DoFn):
    def __init__(self, schema, destination, filename):
        self.full_name = self.get_filename(destination, filename)
        self.schema = schema

    def get_filename(self, destination, filename):
        avro_ext = filename.replace(".csv", ".avro")
        return f"{destination}/{avro_ext}"

    def process(self, element):
        key, records = element
        if len(records) == 0:
            # avoid wirting files without data
            return

        with FileSystems.create(self.full_name) as f:
            fastavro.write.writer(f, self.schema, records)


class RowToDict(DoFn):
    def __init__(self, headers_type, separtor=";"):
        self.headers_type = headers_type
        self.headers_string = separtor.join([h for h in headers_type.keys()])
        self.separtor = separtor

    def tuple_str_to_dict(self, tuple_str):
        if self.headers_string == tuple_str.replace(" ", ""):
            return None
        data = pd.read_csv(
            StringIO(tuple_str),
            sep=self.separtor,
            names=self.headers_type,
            dtype=self.headers_type,
        )
        data_dict = data.to_dict(orient="records")
        return data_dict[0]

    def process(self, element):
        results = []
        for row in element:
            record = self.tuple_str_to_dict(row)
            if record is not None:
                results.append(record)
        return results


def dag_row_to_dict(input_coll, headers_type):
    return (
        input_coll
        | "Todict" >> beam.ParDo(RowToDict(headers_type))
        | "AddKey" >> beam.FlatMap(lambda e: [(1, e)])
        | "GroupByKey" >> beam.GroupByKey()
    )
