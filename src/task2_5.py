from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator

# DummyOperator is deprecated in airflow v2.4.3 and replaced by EmptyOperator.
# Imported EmptyOperator as 'DummyOperator' for consistency with the task
with DAG('task_2_5', start_date=datetime.now()) as dag:
    task1 = DummyOperator(task_id='Task1')
    task2 = DummyOperator(task_id='Task2')
    task3 = DummyOperator(task_id='Task3')
    task4 = DummyOperator(task_id='Task4')
    task5 = DummyOperator(task_id='Task5')
    task6 = DummyOperator(task_id='Task6')

    task1 >> [task2, task3]
    [task2, task3] >> task4
    [task2, task3] >> task5
    [task2, task3] >> task6
