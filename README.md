# mll_truspilot

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
