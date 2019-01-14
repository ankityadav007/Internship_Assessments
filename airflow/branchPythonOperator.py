from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import calendar

default_args={
	'owner':'ankityadav',
	'depends_upon_past':False,
	'retries':1,
	'start_date':datetime(2019,1,14)
}

dag=DAG('batch_operator',default_args=default_args,schedule_interval='@once')

months=["January","February","March","April","May","June","July","August","September","October","November","December"]


def branch():
	for m in months:
		if (m == calendar.month_name[datetime.now().month]):
			return 'task_for_'+m


hello=BranchPythonOperator(
	task_id='branching',
	python_callable=branch,
	provide_context=False,
	dag=dag	
)


for month in months:
	hello.set_downstream(DummyOperator(task_id='task_for_' + month, dag=dag))
