from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
from airflow.models import Variable
import os
from airflow.hooks.S3_hook import S3Hook
# from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

def TTM_rahul():
    print('Hello guys')
    print(2+3)
    print(3==4)

# def Friday_2day():
#     import boto3
#     s3_client=boto3.client('s3')
    # bucket_name='ttmbucketdummy'
    # s3_conn_id='s3_conn'
    # response=s3_client.upload_file('./s3s2s1.csv','ttmbucketdummy','data11/s3s2s1.csv')
    # sensor = S3KeySensor(
    # task_id='check_s3_for_file_in_s3',
    # bucket_key='file-to-watch-*',
    # wildcard_match=True,
    # bucket_name='S3-Bucket-To-Watch',
    # s3_conn_id='my_conn_S3',
    # timeout=18*60*60,
    # poke_interval=120,
    # dag=dag)


# def save_date(ti):
#     date_save =ti.xcom_pull(task_ids=['fetch_date'])
#     if not date_save:
#         raise Exception('No date')
#     df=pd.DataFrame(date_save)
#     csv_path=Variable.get('datesave123')
#     if os.path.exists(csv_path):
#         df_header=False
#         df_mode='a'
#     else:
#         df_header=True
#         df_mode='w'
#     df.to_csv(csv_path, index=False, mode=df_mode,header=df_header)

def upload_to_S3(filename: str, key: str, bucket_name: str)->None:
    hook=S3Hook('s3_conn')
    hook.load_file(filename=filename,key=key,bucket_name=bucket_name)

def fetch_date(ti):
    date=ti.xcom_pull(task_ids=['ddatte'])
    if not date:
        raise Exception('No date')
    date=date[0].split()
    return date[2]

def get_date_now():
    return (str(datetime.now()))

def save_date_now(ti) -> None:
    save_day=ti.xcom_pull(task_ids=['get_date_now'])
    if save_day==None:
        raise ValueError('No dates')
    with open('/home/rahulholla1/Visualcode/apache_airflow/dags/s3s4.txt','w') as f:
        f.write(save_day[0])

def hi_earth():
    import requests
    import json
    response_API = requests.get('https://jsonplaceholder.typicode.com/todos/1')
    data = response_API.text
    parse_json = json.loads(data)
    for k,v in parse_json.items():
        print(f"{k} is assigned to {v}")

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval='@daily',
         catchup=False) as dag:
    task1=PythonOperator(task_id='hello_world',python_callable=TTM_rahul)
    task2=PythonOperator(task_id='hi',python_callable=hi_earth)
    # task3=PythonOperator(task_id='Fri',python_callable=Friday_2day)
    task4=BashOperator(task_id='ddatte',bash_command='date')
    task5=PythonOperator(task_id='fetch_date',python_callable=fetch_date)
    # task6=PythonOperator(task_id='save1_date',python_callable=save_date)
    task7=PythonOperator(task_id='store_xcom',python_callable=get_date_now,do_xcom_push=True)
    # task8=PythonOperator(task_id='save_date_1',python_callable=save_date_now)
    task9=PythonOperator(task_id='upload_s3',python_callable=upload_to_S3,op_args={
        'filename':'/home/rahulholla1/Visualcode/apache_airflow/s3s2s1.csv',
        'key':'s3s2s1.csv',
        'bucket_name':'ttmbucketdummy'})

task1>>task2>>task4>>task5>>task7>>task9

# # default_args={
# # "owner": "airflow",
# # "depends_on_past":False,
# # "start_date":datetime(2019, 1, 1),
# # "retries":0
# # }

# # dag=DAG(dag_id='DAG-1',default_args=default_args,catchup=False,schedule_interval='@daily')

# # start=EmptyOperator(task_id='start',dag=dag)
# # end=EmptyOperator(task_id='end',dag=dag)

# # start>>end
# --------------------------------------------------------------

# from datetime import datetime
# from airflow.models import DAG
# from airflow.operators.python import PythonOperator


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.S3_hook import S3Hook
# import requests
# from datetime import timedelta,datetime

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2022, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0,
# }
# 
# dag = DAG(
#     'add_remote_file_to_s3',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False
# )

# def download_file(url, local_file, **kwargs):
#     response = requests.get(url)
#     open(local_file, "wb").write(response.content)
#     return local_file

# def upload_to_s3(local_file, s3_conn_id, s3_bucket, s3_key, **kwargs):
#     hook = S3Hook(s3_conn_id)
#     hook.load_file(local_file, s3_key, s3_bucket)
#     return f"File {local_file} has been uploaded to S3 bucket {s3_bucket} with key {s3_key}"

# download_task = PythonOperator(
#     task_id='download_file',
#     python_callable=download_file,
#     op_args=['https://drive.google.com/file/d/1--kOS2rKgqgGv3cL8RtOubPiAvKe1pSY/view?usp=share_link', '/tmp/file.txt'],
#     dag=dag,
# )

# upload_to_s3_task = PythonOperator(
#     task_id='upload_to_s3',
#     python_callable=upload_to_s3,
#     op_args=['/tmp/file.txt', 's3_conn', 'ttmbucketdummy', 's1s2s3.csv'],
#     dag=dag,
# )

# download_task >> upload_to_s3_task
