from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
# from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
local_tz = timezone('Asia/Kolkata') 

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,21)),
    'retries': 1,
    'depends_on_past':False,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dependent_dag_1',
    default_args=default_args,
    description='This is the dependent dag to execute',
    schedule_interval=timedelta(days=1),
) as dag:
    task1 = BashOperator(
    task_id='task1',
    # bash_command='echo "task01 has been executed from the dependent_dag_1"',
    bash_command='sleep 45',
    )
    task1
     

