# mll_truspilot code challenge

# Ingestion
The ingestion could be done pretty simply, but to demostrate my experience with Apache Beam
I wrote code to transform the CSV to AVRO, this may be an overkill solution, but for complex transformation
in Bigdata Beam is a great tool, also it can be used for streaming data, and hosted in GCP Dataflow.

Avro is used as an intermediate materialization, that can be uploaded to the Data Lake
and then to Data Waterhouse, where simpler transformations can be done using SQL

## Sample data_
The CSV sample file is in the directory sample_data_01.csv

## Apache Beam Code
The Apache Beam code is in etl_beam, it has 3 files, transforms.py and test_transforms.py have the basic code
the etl_beam.py has the code with parameters for the input and output file path.

## INSTALL
```
    make install
```

## Testing
unit test has been writen and added in github workflows, best to run a coverage and aim to high test coverage
```
    make coverage
```

## Lint and formating
To have a formated and code with the guidelines use:
```
    make lint
```

## Running:
using the default parameters
```
    python etl_beam/etl_csv_to_avro.py
```

```
    python etl_beam/etl_csv_to_avro.py \
        --schema_path OTHER \
        --input_file OTHER \
        --destination OTHER
```


# INGEST DATA: Datawerehouse BigQuery

## Resouces
Some resources were created direcly in the GCP outside this repe:
* project_id
* dataset

## Requirements
gcloud skd

## One time load
```
    source load_to_bq/variables.sh
    make print_variables
    make bq_load
```
