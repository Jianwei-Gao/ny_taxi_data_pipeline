# dbt-bigquery set up in docker

build image by:
- docker compose build

or if not cd in dbt folder 
- docker compose -f \<path to dbt/docker-compose.yaml\> build

execute cml by
- docker compose \<-f to path if needed\> run [options] dbt-bigquery [command] [args...]
- for example: 
    1. docker compose run dbt-bigquery dbt init
    2. docker compose run dbt-bigquery bash
