#1.
export AIRFLOW_UID=$(id -u)
cd repo/
docker-compose -f airflow/docker-compose.yaml build
docker-compose -f airflow/docker-compose.yaml up


#2.
- goto localhost:8080
- find the graph
- execute the graph
- the database will appear in airflow/data/

