# Instructions
1. Run start.sh to execute the docker-compose.
2. In ```/airflow-docker-hdfs-spark-example/mnt/airflow/airflow.cfg``` , fill in ```smtp_host```, ```smtp_user```, ```smtp_password```, ```smtp_mail_from``` under the ```smtp``` tag. 
3. Type ```docker ps``` in command line to view running containers.
4. You can access the Airflow UI by going to http://localhost:8080 . Turn the DAG on, and trigger it. 
5. Run stop.sh to stop the containers. 

# Illustration of DAG
The purpose of this ETL pipeline is to pull Willshire 5000 rates on a nightly basis. The Willshire 5000 Index is an indicator of Stock Market Capitalization (SMC). 

![Test Image 6](https://github.com/angelotc/airflow-docker-hdfs-spark-example/blob/master/dag-example.png)

