# How to run the assignment
1. `poetry init` to install requirements
2. `docker-compose up` to start postgres database
3. `poetry run python ingest_data.py` to upload data (part 1 of exercise)
3. `poetry run python aggregate_data.py` to aggregate data (part 2 of exercise)