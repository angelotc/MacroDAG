from airflow import DAG,settings
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Connection
import json
import csv
import requests
import os


## DAG STUFF 
default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020, 8, 22),
    "depends_on_past" :  False,
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email" : "angelocortez102@gmail.com",
    "retries" : 1,
    "retry_delay" : timedelta(minutes = 5) 
}

def download_willshire_rates():
    # SAVE URL -> CSV
    temp_file_name = 'willshire.csv'
    # 20 years ago
    start_date = (datetime.now().date())
    end_date = ((datetime.now() - timedelta(days=20*365)).date())
    CSV_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?mode=fred&recession_bars=on&ts=12&tts=12&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=WILL5000PRFC&scale=left&cosd={}&coed={}&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily%2C%20Close&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-09-08&revision_date=2020-09-08&nd=1999-12-31%22'.format(str(start_date),str(end_date))
    data = {}
    response = requests.get(CSV_URL)
    with open(os.path.join("/usr/local/airflow/dags/files/", temp_file_name), 'wb') as f:
        f.write(response.content)
    f.close()

    #Print URL
    with open(os.path.join("/usr/local/airflow/dags/files/", temp_file_name), 'r') as readfile:
        reader = csv.DictReader(readfile, delimiter=',')
        
        for row in reader:
            print(row)

def download_gdp_monthly_rates():
    # SAVE URL -> CSV
    temp_file_name = 'gdp_quarterly.csv'
    # 20 years ago
    start_date = (datetime.now().date())
    end_date = ((datetime.now() - timedelta(days=20*365)).date())
    CSV_URL = 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDPC1&scale=left&cosd={}&coed={}&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-09-30&revision_date=2020-09-30&nd=1947-01-01
'.format(str(start_date),str(end_date))
    data = {}
    response = requests.get(CSV_URL)
    with open(os.path.join("/usr/local/airflow/dags/files/", temp_file_name), 'wb') as f:
        f.write(response.content)
    f.close()

    #Print URL
    with open(os.path.join("/usr/local/airflow/dags/files/", temp_file_name), 'r') as readfile:
        reader = csv.DictReader(readfile, delimiter=',')
        
        for row in reader:
            print(row)



with DAG(dag_id = "willshire_dag", 
        schedule_interval = "@daily",
        default_args = default_args, 
        catchup = False) as dag:

    is_willshire_5000_available = HttpSensor(
        task_id = "is_willshire_5000_available",
        method = "GET",
        http_conn_id = "willshire_api",
        endpoint = 'latest',
        response_check = lambda response: "DATE" in response.text,
        poke_interval = 5,
        timeout=20
    )
    is_gdp_rates_available = HttpSensor(
        task_id = "is_willshire_5000_available",
        method = "GET",
        http_conn_id = "gdp_api",
        endpoint = 'latest',
        response_check = lambda response: "DATE" in response.text,
        poke_interval = 5,
        timeout=20
    )

    downloading_willshire_rates = PythonOperator(
        task_id="downloading_willshire_rates",
        python_callable=download_willshire_rates
    )
    download_gdp_monthly_rates = PythonOperator(
        task_id="download_gdp_monthly_rates",
        python_callable=download_gdp_monthly_rates
        
    is_willshire_file_available = FileSensor(
        task_id="is_willshire_file_available",
        fs_conn_id="willshire_path",
        filepath="willshire.csv",
        poke_interval=5,
        timeout=20
    )

    is_gdp_file_available = FileSensor(
        task_id="is_gdp_file_available",
        fs_conn_id="gdp_path",
        filepath="gdp_quarterly.csv",
        poke_interval=5,
        timeout=20
    )

    saving_willshire_rates = BashOperator(
        task_id = "saving_willshire_rates",
        bash_command = """
            hdfs dfs -mkdir -p /willshire && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/willshire.csv /willshire
        """
    )
    saving_gdp_rates = BashOperator(
        task_id = "saving_gdp_rates",
        bash_command = """
            hdfs dfs -mkdir -p /gdp_quarterly && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/gdp_quarterly.csv /gdp_quarterly
        """
    )
    creating_willshire_base_table = HiveOperator(
        task_id="creating_willshire_base_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS willshire_base_table(
                `date` DATE,
                will5000 DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
        )
    creating_willshire_incremental_table = HiveOperator(
        task_id="creating_willshire_incremental_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS willshire_incremental_table(
                `date` DATE,
                will5000 DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
        )

    creating_gdp_base_table = HiveOperator(
        task_id="creating_gdp_base_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS gdp_base_table(
                `date` DATE,
                gdp DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
        )
    creating_gdp_incremental_table = HiveOperator(
        task_id="creating_gdp_incremental_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS gdp_incremental_table(
                `date` DATE,
                gdp DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
        )

    populate_willshire_incremental = SparkSubmitOperator(
        task_id = "populate_willshire_incremental",
        conn_id = "spark_conn",
        application = "/usr/local/airflow/dags/scripts/willshire_incremental_populate.py",
        verbose = False
    )
    
    populate_willshire_base = SparkSubmitOperator(
        task_id = "populate_willshire_base",
        conn_id = "spark_conn",
        application = "/usr/local/airflow/dags/scripts/willshire_base_populate.py",
        verbose = False
    )

    # impute_willshire_zeros = SparkSubmitOperator(
    #     task_id = "impute_willshire_zeros",
    #     conn_id = "spark_conn",
    #     application = "/usr/local/airflow/dags/scripts/impute_willshire_zeros.py",
    #     verbose = False
    # )
    delete_willshire_incremental_rows = SparkSubmitOperator(
        task_id = "delete_willshire_incremental_rows",
        conn_id = "spark_conn",
        application = "/usr/local/airflow/dags/scripts/willshire_delete_incremental.py",
        verbose = False
    )
    # sending_email = EmailOperator(
    #     task_id="sending_email",
    #     to="angelocortez102@gmail.com",
    #     subject="forex_data_pipeline",
    #     html_content="""
    #         <h3>forex_data_pipeline succeeded</h3>
    #     """
    #     )
    is_willshire_5000_available >> downloading_willshire_rates >> is_willshire_file_available >> saving_willshire_rates 
    saving_willshire_rates >> [creating_willshire_base_table, creating_willshire_incremental_table] >> populate_willshire_incremental 
    populate_willshire_incremental >> populate_willshire_base >> delete_willshire_incremental_rows