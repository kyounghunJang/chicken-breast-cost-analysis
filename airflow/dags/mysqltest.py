from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
default_args={
    "owner":"airflow", 
    'depends_on_past' : False,
    "start_date":datetime(2022,8,10),
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
    }

def task_test_query():
    mysql = MySqlHook(mysql_conn_id='mysql')
    rows= mysql.get_records("SELECT * FROM info ")

    for row in rows:
        print(row)
        
with DAG(dag_id="mysqltest", default_args=default_args, schedule_interval='55 14 * * *') as dag:
        
        crawli1 = PythonOperator(
        task_id="cr1",
        provide_context=True,
        python_callable=task_test_query,
    )