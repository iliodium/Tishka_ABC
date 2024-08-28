#!/bin/bash

# Переходим в папку с docker-compose файлами
cd ~/tishka || exit

# Запускаем docker-compose для каждого файла
#docker compose -f rabbitmq/docker-compose.yaml --env-file rabbitmq/.env up -d
#docker compose -f apache_airflow/docker-compose.yaml --env-file envs/.env.apache_airflow_port --env-file envs/.env.apache_airflow_dns --env-file apache_airflow/.env up -d
# docker compose -f services/docker-compose.yaml up -d
#docker compose -f config_server/docker-compose.yaml up -d
docker compose -f telegram_bot/docker-compose.yaml --env-file telegram_bot/.env up -d


#source envs/.env.rabbitmq_user_log_pass
#docker exec rabbitmq rabbitmqctl add_user $RABBITMQ_USERNAME $RABBITMQ_PASSWORD
#docker exec rabbitmq rabbitmqctl set_permissions -p / $RABBITMQ_USERNAME ".*" ".*" ".*"
#
#source apache_airflow/.env
#docker exec airflow-webserver airflow roles create DAGExecutor
# can edit on DAG Runs, can edit on DAGs, can read on Website, can create on DAG Runs
#docker exec airflow-webserver airflow users create# --username "$AIRFLOW_USER_USERNAME" --firstname None --lastname None --password "$AIRFLOW_USER_PASSWORD" --role DAGExecutor --email None
