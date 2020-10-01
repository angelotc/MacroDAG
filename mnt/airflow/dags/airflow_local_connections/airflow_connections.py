import json
import logging
from airflow import models
from airflow.utils import db
MY_CONFIG = json.dumps({
    "path": "/usr/local/airflow/dags/files"
})


def add_connection( conn_id=None, conn_type=None, host=None, extra=None, login=None, password=None, port=None):
    logging.info("Adding connection: {}".format(conn_id))
    m = models.Connection(conn_id=conn_id, conn_type=conn_type, host = host, extra=extra,  login=login, password = password, port = port)
    db.merge_conn(m)

if __name__ == "__main__":
    add_connection('willshire_api', 'http', "https://fred.stlouisfed.org/graph/fredgraph.csv?mode=fred&recession_bars=on&ts=12&tts=12&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=WILL5000PRFC&scale=left&cosd=2019-06-02&coed=2020-09-04&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily%2C%20Close&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-09-08&revision_date=2020-09-08&nd=1999-12-31%22" )
    add_connection('gdp_api', 'http', "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=off&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GEPUCURRENT&scale=left&cosd=1997-01-01&coed=2020-07-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2020-09-30&revision_date=2020-09-30&nd=1997-01-01" )
    add_connection("willshire_path", "fs","", MY_CONFIG,"")
    add_connection("gdp_path", "fs","", MY_CONFIG,"")
    add_connection( "spark_conn", "","spark://spark-master",  port=7077)
    add_connection( "hive_conn", "hiveserver2","hive-server", None,"hive","hive", 10000)

