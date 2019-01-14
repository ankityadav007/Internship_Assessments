import logging
import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

os.environ['SPARK_HOME'] = '/usr/local/spark/'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args={	
	'owner':'ankityadav',
	'depends_upon_past':False,
	'start_date':datetime(2019,01,03),
	'email':['xyz@gmail.com'],
	'email_on_failure':False,
	'email_on_retry':False,
	'retries':1,
	'retry_delay':timedelta(minutes=1),
}

dag=DAG('new_spark',default_args=default_args)
task1 = BashOperator(
    task_id='firstA',
    bash_command='spark-submit --master local[*] --deploy-mode client /home/ankityadav/pyspark/hadoop_spark/firstA.py',
    trigger_rule='all_done',
    dag=dag
)

task2 = BashOperator(
    task_id='secondA',
    bash_command='spark-submit --master local[*] --deploy-mode client /home/ankityadav/pyspark/hadoop_spark/secondA.py', 
    trigger_rule='all_done',
    dag=dag
)

task3 = BashOperator(
    task_id='thirdA',
    bash_command='spark-submit --master local[*] --deploy-mode client /home/ankityadav/pyspark/hadoop_spark/thirdA.py',
    trigger_rule='all_done',
    dag=dag
)

task1 >> task2 >>task3

#task2.setUpstream(task1)
#task3.setUpstream(task1)
#task3.setUpstream(task2)
