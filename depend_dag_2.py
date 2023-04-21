from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
local_tz = timezone('Asia/Kolkata') 

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,4,21)),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dependent_dag_2',
    default_args=default_args,
    description='This is the dependent dag to execute',
    schedule_interval=timedelta(days=1),
) as dag:
    externalsensor=ExternalTaskSensor(
        task_id='externalsensor',
        external_dag_id='dependent_dag_1',
        external_task_id='task1',
        execution_delta=timedelta(seconds=15)
        # poke_interval=timedelta(seconds=15)

    )

    task2 = BashOperator(
    task_id='task02',
    bash_command='echo "task02 will be executed from the dag2"',
    )
    externalsensor>>task2
     

