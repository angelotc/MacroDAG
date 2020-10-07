# Instructions
1. Run start.sh to execute the docker-compose (you can do this in command prompt by typing ```./start.sh```). 
2. *Optional:* In ```/airflow-docker-hdfs-spark-example/mnt/airflow/airflow.cfg``` , fill in ```smtp_host```, ```smtp_user```, ```smtp_password```, ```smtp_mail_from``` under the ```smtp``` tag. 
3. Type ```docker ps``` in command line to view running containers. Copy the Container ID for Airflow
4. You can access the Airflow UI by going to http://localhost:8080 . Turn the DAG on, and trigger it. 
# Starting your connections
5. In command prompt, type ```docker exec -it <your Airflow Container ID> /bin/bash```.
6. You are now in your Airflow container. Type ```cd usr/local/airflow/dags/airflow_local_connections``` in command prompt, and start your connections by typing in ```python3 airflow_connections.py```
# Stopping your containers
7. Exit your Airflow container by pressing Ctrl + D. 
8. Run stop.sh ( type ```./stop.sh``` ). 


# Illustration of Macro DAG
The purpose of this ETL pipeline is to pull the following macroeconomic indicators:
- Willshire 5000 on a nightly basis. 
- interest rates on a nightly basis. 
- GDP rates on a quarterly basis. The frequency will be interpolated into a daily frequency using a variety of methods (linear interpolation, knn, spline, ensemble methods).


Each rate will be pulled in such a fashion: 

![Test Image 6](https://github.com/angelotc/airflow-docker-hdfs-spark-example/blob/master/dag-example.png)

# To-do list (in order of priority)
1. Create DAG for interest rates
2. Deploy on AWS.
4. Calculate Buffett Indicator using (WILLSHIRE5000/Daily interpolated GDP)
5. Estimation using fbprophet

