import argparse
import logging

import apache_beam as beam
import fastavro
from apache_beam import Pipeline, io
from apache_beam.options.pipeline_options import PipelineOptions
from transforms import WriteToAvro, dag_row_to_dict

SCHEMA = {}  # TODO
HEADERS_TYPE = {f["name"]: f["type"] for f in SCHEMA["fields"]}
PARSED_SCHEMA = fastavro.schema.parse_schema(SCHEMA)


def run(input_path, input_file, destination, pipeline_args=None):

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    pipeline = Pipeline(options=pipeline_options)

    input_coll = pipeline | "ReadFile" >> io.ReadFromText(f"{input_path}/{input_file}") | beam.FlatMap(lambda x: [[x]])

    dict_coll = dag_row_to_dict(input_coll)
    dict_coll | "write-avro2" >> beam.ParDo(WriteToAvro(destination, input_file))
    dict_coll | "print2" >> beam.Map(print)

    result = pipeline.run()

    return result


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="./sample_data")
    parser.add_argument("--input_file", default="reviews_01.csv")
    parser.add_argument("--destination", default="./avro_files")
    parser.add_argument("--log_level", type=int, help="Python log level", default=30)

    known_args, pipeline_args = parser.parse_known_args()
    logging.getLogger().setLevel(known_args.log_level)
    logging.info(f"known_args:{known_args}")
    logging.info(f"pipeline_args:{pipeline_args}")

    run(
        known_args.input_path,
        known_args.input_file,
        known_args.destination,
        pipeline_args,
    )


if __name__ == "__main__":
    main()
