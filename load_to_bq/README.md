# Datawerehouse BigQuery

## Resouces
Some resources were created direcly in the GCP outside this repe:
* project_id
* dataset

## Requirements
gcloud skd

# One time load
```
    cd load_to_bq
    source variables.sh
    source load_to_bq/variables.sh
    make print_variables
    make bq_load
```
