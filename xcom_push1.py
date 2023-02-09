from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {'owner':'rahul'}

def info(ti):
    student_first_name = ti.xcom_pull(task_ids='first_name',key='student_first_name')
    student_class = ti.xcom_pull(task_ids='first_name',key='student_class')
    student_specialization = ti.xcom_pull(task_ids='first_name',key='student_specialization')
    print(f"Mr/Ms.{student_first_name} of class {student_class} has chosen {student_specialization} as the prefered subject")

def first_name(ti):
    ti.xcom_push(key='student_first_name',value='Rahul')
    ti.xcom_push(key='student_class',value='12')
    ti.xcom_push(key='student_specialization',value='Electronics')

with DAG(
    dag_id = "python_operator_dag",
    default_args = default_args,
    start_date = datetime(2023, 1, 23),
    schedule_interval = "@daily",
    description = "performing python operators"
) as dag:
    task1 = PythonOperator(
        task_id = "info",
        python_callable = info,
    )
    task2 = PythonOperator(
        task_id = "first_name",
        python_callable = first_name
    )
    task2 >> task1