from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import numpy as np
import pandas as pd
import re

default_args = {
    "owner": "kuanysh",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

INPUT_FILE = '/opt/airflow/dataset/tiktok_google_play_reviews.csv'
OUTPUT_FILE = '/opt/airflow/dataset/processed_file.csv'

def replace_null_values(input_file, output_file):
    data = pd.read_csv(input_file)
    data = data.replace(np.nan, '-')
    data.to_csv(output_file, index=False)

def sort_data(input_file, output_file):
    data = pd.read_csv(input_file)
    data = data.sort_values(by='at')
    data.to_csv(output_file, index=False)

def clean_content_column(input_file, output_file):
    data = pd.read_csv(input_file)
    data['content'] = data['content'].str.replace('[^a-zA-Z0-9\s]', '', regex=True)
    data.to_csv(output_file, index=False)

with DAG(
    default_args = default_args,
    dag_id = "dag_with_sensor_dataset51",
    description = "This DAG begin TaskGroups",
    start_date = datetime(2023, 10, 13)
) as dag:
    @task_group(
        group_id="task_group_with_with_preprocessing",
        default_args = default_args,
        tooltip = "This task group is very important!"
    )
    def tg1():
        file_sensor_task = FileSensor(
            task_id='file_sensor_task',
            fs_conn_id = 'fs_conn',
            filepath='/opt/airflow/dataset/tiktok_google_play_reviews.csv',
            poke_interval=5,
            timeout=20
        )
        null_replace_task = PythonOperator(
            task_id="null_replace_task",
            python_callable=replace_null_values,
            op_args=[INPUT_FILE, OUTPUT_FILE]
        )

        sort_task = PythonOperator(
            task_id="sort_task",
            python_callable=sort_data,
            op_args=[OUTPUT_FILE, OUTPUT_FILE]
        )

        delete_symbols = PythonOperator(
            task_id="clean_content_column",
            python_callable=clean_content_column,
            op_args=[OUTPUT_FILE, OUTPUT_FILE]
        )

        file_sensor_task >> null_replace_task >> sort_task >> delete_symbols


    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")

    start_task >> tg1() >> end_task
