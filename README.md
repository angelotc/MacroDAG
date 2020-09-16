# Instructions
1. Run start.sh to execute the docker-compose.
2. Type ```docker ps``` in command line to view running containers.
3. In ```/airflow-docker-hdfs-spark-example/mnt/airflow/airflow.cfg``` , include your SMTP credentials under the ```smtp``` tag. 
4. You can access the Airflow UI by going to http://localhost:8080 . Turn the DAG on, and trigger it. 
5. Run stop.sh to stop the containers. 

# Illustration of DAG. Its purpose is to pull Willshire 5000 rates on a nightly basis. 

![Test Image 6](https://github.com/angelotc/airflow-docker-hdfs-spark-example/blob/master/dag-example.png)

