from airflow.operators.mysql_operator import MySqlOperator
from airflow import DAG
from datetime import datetime,timedelta
import pymysql
pymysql.install_as_MySQLdb()

default_args={"owner":"airflow", "start_date":datetime(2022,8,11)}
with DAG(dag_id="workflow", default_args=default_args, schedule_interval='@daily') as dag:
    
    create_table =MySqlOperator(
        task_id ="create_table",
        mysql_conn_id="mysql_db" ,
        sql="CREATE DATABASE qweql",
    )

    create_table