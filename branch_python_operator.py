from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime

def lowest_marks_finder_exam(ti):
    student_marks_list = ti.xcom_pull(task_ids=['student_1','student_2','student_3'])
    lowest_marks = max(student_marks_list)
    if (lowest_marks > 35):
        return 'Passed'
    return 'Failed'

def marks_generator():
    return randint(0,100)

with DAG("Exam_is_coming",start_date=datetime(2023, 2, 2),
    schedule_interval="@daily",catchup=False) as dag:
        student_1 = PythonOperator(
            task_id = "student_1",
            python_callable = marks_generator
        )
        student_2 = PythonOperator(
            task_id = "student_2",
            python_callable = marks_generator
        )
        student_3 = PythonOperator(
            task_id = "student_3",
            python_callable = marks_generator
        )
        lowest_marks_finder = BranchPythonOperator(
            task_id = "lowest_marks_finder",
            python_callable = lowest_marks_finder_exam
        )
        Passed = BashOperator(
            task_id = "Passed",
            bash_command = "echo 'Everone has passed the exam'"
        )
        Failed = BashOperator(
            task_id = "Failed",
            bash_command = "echo 'Everyone has failed the exam'"
        )
        [student_1, student_2, student_3] >> lowest_marks_finder >> [Passed, Failed]