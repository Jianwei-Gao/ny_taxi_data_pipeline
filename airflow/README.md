My docker set up for airflow\
Connects to my google cloud account for orchestration on google cloud storage and bigquery 

cd directory should be the root project folder\
build custom image by \
docker build -t \<tag name\> -f ./airflow/dockerfile .

build container by (in the following order)\
docker compose -f ./airflow/docker-compose.yaml up airflow-init \
docker compose -f ./airflow/docker-compose.yaml up

or cd into airflow folder then \
docker compose airflow-init
docker compose up