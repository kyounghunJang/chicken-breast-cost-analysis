from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime,timedelta
import pymysql
from selenium.webdriver.chrome.options import Options
from selenium import webdriver                                 
from webdriver_manager.chrome import ChromeDriverManager       
from selenium.webdriver.common.by import By                    
from time import sleep                                         
import pandas as pd                                        
import pyspark                                                 
from pyspark.sql import SparkSession                          
chrome_options = Options()        
chrome_options.add_argument("--no-sandbox")                 
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-dev-shm-usage')           
                                                             
from selenium.webdriver import ActionChains                    
from selenium.webdriver.common.keys import Keys                
from selenium.common.exceptions import NoSuchElementException  
from multiprocessing.pool import ThreadPool
import multiprocessing                                  
from pyspark.sql.functions import regexp_replace, col          
from datetime import datetime     
                                                      
default_args={
    "owner":"airflow", 
    'depends_on_past' : False,
    "start_date":datetime(2022,8,10),
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
    }
def connectMysql():
    mysql_db = pymysql.connect(
    user='root', 
    passwd='root', 
    host='127.0.0.1', 
    db='pymysql_db', 
    charset='utf8')
    cursor=mysql_db.cursor
    return cursor
    
def crawling(*op_args):
    cursor=connectMysql()
    sql="INSERT INTO data (image,name,price,review) VALUES (%s, %s,%s %s)"
    s,e=op_args
    driver=webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    driver.implicitly_wait(3)
    action=ActionChains(driver)
    driver.get('https://search.shopping.naver.com/search/all?query=%ED%82%A4%EB%B3%B4%EB%93%9C&frm=NVSHATC&prevQuery=%ED%82%A4%EB%B3%B4%EB%93%9C')
    sleep(5)
    driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/div[1]/ul/li[2]/a').click()
    sleep(5)
    total=0
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    sleep(5)
    driver.execute_script("window.scrollTo(0,10)")
    sleep(5)
    num = 0
    while True:
        for i in range(s,e):
            scroll=driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div[{}]/li/div/div[2]/div[1]/a'.format(i))
            action.move_to_element(scroll).perform()
            sleep(2)
            image=driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div[{}]/li/div/div[1]/div/a/img'.format(i)).get_attribute('src')
            price=driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div[{}]/li/div/div[2]/div[2]/strong/span/span[2]'.format(i)).text
            name=driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div[{}]/li/div/div[2]/div[1]/a'.format(i)).text
            driver.find_element(By.XPATH,'//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div[{}]/li/div/div[2]/div[1]/a'.format(i)).click()
            sleep(5)
            driver.switch_to.window(driver.window_handles[-1])
            chk=0
            cnt=0
            review=''
            while True:
                try:
                    pages=driver.find_element(By.CSS_SELECTOR,'#section_review > div.pagination_pagination__2M9a4').text
                    pages=pages.replace('현재 페이지',"")
                    if '이전' in pages:
                        start=2
                        end=13
                    else:
                        start=1
                        end=12
                    for j in range(start,end):
                        try:
                            driver.find_element(By.XPATH, '//*[@id="section_review"]/div[3]/a[{}]'.format(j)).click()
                            sleep(3)
                            tmp=driver.find_elements(By.CLASS_NAME,'reviewItems_text__XIsTc')
                            cnt+=1
                        except NoSuchElementException:
                            chk=1
                            break
                except NoSuchElementException:
                    chk=1
                    tmp=driver.find_elements(By.CLASS_NAME,'reviewItems_text__XIsTc')
                sleep(5)
                for rv in tmp:
                    review+=rv.text
                if chk==1 or cnt>29:
                    break
            cursor.execute(sql,(image,name,price,review))
            driver.close()
            sleep(5)
            driver.switch_to.window(driver.window_handles[0])
        driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/div[3]/a').click()
        num+=1
        print(num)
        if num==3:
            break
        sleep(5)
    cursor.commit()
    cursor.close()
    driver.quit()
    
    
with DAG(dag_id="craw", default_args=default_args, schedule_interval='55 14 * * *') as dag:
    crawli1 = PythonOperator(
        task_id="cr1",
        provide_context=True,
        python_callable=crawling,
        op_args=(1,2),
    )
    crawli2 = PythonOperator(
        task_id="cr2",
        provide_context=True,
        python_callable=crawling,
        op_args=(2,3),
    )
    
(crawli1,crawli2)
