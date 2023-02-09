from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json,requests

def s34():
    s3_hook= S3Hook(aws_conn_id='s3_conn')
    s3_hook.load_file(
        filename='dags/s3s2s1.csv',
        key='s3s2s1.csv',
        bucket_name='ttmbucketdummy',
        replace=True)

def TTM_rahul():
    print('Hello guys',2+3,3==4)

def hi_earth():
    response_API = requests.get('https://jsonplaceholder.typicode.com/todos/1')
    data = response_API.text
    parse_json = json.loads(data)
    for k,v in parse_json.items():
        print(f"{k} is assigned to {v}")

def get_date_now():
    return (str(datetime.now()))

def fetch_date(ti):
    date=ti.xcom_pull(task_ids=['abc'])
    if date==None:
        raise Exception('No date')
    date=date[0].split()
    return (f"today's date is {date[0]}")

with DAG(dag_id="hello_beautiful_world123",
         start_date=datetime(2021,1,1),
         schedule_interval='@daily',
         catchup=False) as dag:
    task7=BashOperator(task_id='asd',bash_command='echo Start the program')
    task1=PythonOperator(task_id='hello_beautiful',python_callable=s34)
    task2=PythonOperator(task_id='hi_earth_1',python_callable=hi_earth)
    task3=PythonOperator(task_id='TM_rahul',python_callable=TTM_rahul)
    task4=PythonOperator(task_id='abc',python_callable=get_date_now)
    task5=PythonOperator(task_id='ddaattee',python_callable=fetch_date)
    task6=BashOperator(task_id='xyz',bash_command='echo End of program')

task7>>task1>>[task2,task3,task4]>>task5>>task6