import random 
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args={
	'owner':'airflow',
	'start_date':datetime(2019,4,18)
	'depends_on_past':False,
	'retries':1
}

dag=DAG(dag_id='branch_operator',default_args=args,schedule_interval="@once")

days=["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

def get_day(**kwargs):
	kwargs[ti].xcom_push(key='day',value=datetime.now().weekday())

def branch(**kwargs):
	return 'task_for_'+days[kwargs['ti'].xcom_pull(tasks_ids='weekday',key='day')]

get_weekday=PythonOperator(
	task_id='weekday',
	python_callable=get_day,
	provide_context=True,
	dag=dag
)

hello=BranchPythonOperator(
	task_id='branching',
	python_callable=branch,
	provide_context=True,
	dag=dag
)

get_weekday.set_downstream(fork)

for day in range(0,6):
	hello.set_downstream(DummyOperator(task_id='task_id_'+days[day], dag=dag))
