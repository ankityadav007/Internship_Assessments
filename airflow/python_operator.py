from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import pprint
import time

default_args={
	'owner':'ankityadav',
	'depends_upon_past':False,
	'start_date':datetime(2019,01,03)
}

dag=DAG('python_operator',default_args=default_args)

def print_context(ds, **kwargs):
    pprint.pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'
	

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

def my_sleeping_function(random_base):
	time.sleep(random_base)

for i in range(5):
	task=PythonOperator(
		task_id='sleep_for_'+str(i),
		python_callable=my_sleeping_function,
		op_kwargs={'random_base':float(i)/10},
		dag=dag,
)

run_this >> task
