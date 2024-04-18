# Ingestion
ingestion could be done using micro batching (per day, hour) from avro to permanent storage
and have a view of the latest data (if is streaming or comes pretty frequently).

Depending on the usage create partition or index, ingested date, or creation date.

This can be orchestrated with Airflow

# Data cleaning
Good practice in Datawerehouse is to have a raw layer, and from there use sql to clean the data
for example using regex to determine a valid email, check that the review rating is within the range, etc

# Normalization
the transaction database in SQL is usually normalized, so a table for reviewers, a table for locations_revied
and a table with reviews, that will have primary keys, foreign keys, etc.

# CD deployment
Depending on the actual implementacion deplyment should be implemented on when a pull request is completed
