from airflow.decorators import dag,task
from datetime import datetime

default_args = {'owner':'rahul'}
@dag(dag_id='taskflow_Trial',
        default_args=default_args,
        start_date=datetime(2023, 1, 13),
        schedule_interval="@daily")

def taskflow_api():
    @task()
    def time_value():
        time=str(datetime.now())
        time=time.split() 
        return (time[1])
    
    @task()
    def date_value():
        date=str(datetime.now())
        date=date.split()
        return (date[0]) 
    
    @task()
    def today_time_date(time,date):
        print(f"Today's date is {date} and the time now is approx {time}")
    time = time_value()
    date = date_value()
    today_time_date(time=time,date=date)
o1= taskflow_api()