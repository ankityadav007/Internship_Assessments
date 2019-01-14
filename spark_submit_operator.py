from airflow.operators import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow import DAG



default_args = {
   	'owner':'ankityadav',
	'email':['xyz@gmail.com'],
	'depends_upon_past':False,
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),

}
dag = DAG('spark_submit_operator', default_args=default_args, start_date=(datetime(2019,01,03)))

task1 = SparkSubmitOperator(
	task_id='first',
	application_file="/home/hduser/airflow/dags/files/firstA.py",
	master='yarn',
	dag=dag
)

task2 = SparkSubmitOperator(
	task_id='second',
	application_file="/home/hduser/airflow/dags/files/secondA.py",
	master='yarn',
	dag=dag
)

task3 = SparkSubmitOperator(
	task_id='third',
	application_file="/home/hduser/airflow/dags/files/thirdA.py",
	master='yarn',
	dag=dag
)




task1 >> task2 >> task3

