.PHONY: install install-dev test

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -d -name '__pycache__' -exec rmdir {} +

clean: clean-pyc
	rm -rf .pytest_cache
	rm -rf *.egg-info


install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

coverage:
	coverage run -m pytest
	coverage report -m > coverage_report.txt
	coverage html
	open htmlcov/index.html

# LOAD to BigQuery
print_variables:
	echo GCP_PROJECT_ID:${GCP_PROJECT_ID}
	echo BQ_DATASET:${BQ_DATASET}

bq_load:
	bq load --source_format="AVRO" --autodetect --replace ${GCP_PROJECT_ID}:${BQ_DATASET}.reviews_01 avro_files/reviews_01.avro
