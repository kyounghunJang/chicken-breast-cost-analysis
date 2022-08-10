from selenium.webdriver.chrome.options import Options
from selenium import webdriver                                 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
chrome_options = Options()       
chrome_options.add_argument("--no-sandbox")
chrome_options.binary_location='/usr/bin/google-chrome-stable'                  
chrome_options.add_argument("--headless")          
default_args={"owner":"airflow", "start_date":datetime(2022,8,8)}

def ttt():
    driver=webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    driver.get('https://search.shopping.naver.com/search/all?query=%ED%82%A4%EB%B3%B4%EB%93%9C&frm=NVSHATC&prevQuery=%ED%82%A4%EB%B3%B4%EB%93%9C')

with DAG(dag_id="fuck", default_args=default_args, schedule_interval='@daily') as dag:
    
    create_table =PythonOperator(
        task_id ="fucking",
        python_callable=ttt,
        provide_context=True,

    )
    create_table
