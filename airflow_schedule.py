"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
# sys.path.insert(0, '/home/PycharmProject/GTM/get_tag_list.py')
# from get_tag_list import main

default_args = {
    'owner': 'GTMChecking',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 2),
    'email': ['wy790054473@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# def test():
#     print("just a test")


dag = DAG('testforgtm', default_args=default_args, schedule_interval=timedelta(days=1))





# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='check_tags_without_ot_triggers',
    owner='flipped',
    bash_command='python /home/flipped/PycharmProjects/GTM/get_tag_list.py',
    dag=dag)

#
# def main():
#     tt = PythonOperator(task_id='check_tags_without_ot_triggers', provide_context=True, python_callable=test, dag=dag)
#
if __name__ == '__main__':
    dag.cli()