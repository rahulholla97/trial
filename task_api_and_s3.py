from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hi_earth():
    import requests
    import json
    response_API = requests.get('https://jsonplaceholder.typicode.com/todos/1')
    data = response_API.text
    parse_json = json.loads(data)
    for k,v in parse_json.items():
        print(f"{k} is assigned to {v}")

with DAG(dag_id="xyz123",
         start_date=datetime(2021,1,1),
         schedule_interval='@daily',
         catchup=False) as dag:
    task1=PythonOperator(task_id='hello_world',python_callable=hi_earth)