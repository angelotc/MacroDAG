#!/bin/bash

# Build the base images from which are based the Dockerfiles
# then Startup all the containers at once 
python ~/Desktop/Projects/airflow-docker-hdfs-spark-example/mnt/airflow/dags/airflow_local_connections/airflow_connections.py