import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import datetime

@dag(
    dag_id="load_data_to_mongodb3",
    schedule=None,
    start_date=datetime(2022, 10, 28),
    catchup=False,
    default_args={
        "retries": 0,
    },
)
def load_data_to_mongodb():
    @task
    def upload_to_mongo():
        df = pd.read_csv('/opt/airflow/dataset/processed_file.csv')
        json_data = df.to_dict(orient='records')

        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.MyDB
        airflow_task = db.airflow_task
        airflow_task.insert_many(json_data)
        #print(f"Loaded {len(data)} records into MongoDB")

    upload_to_mongo()

load_data_to_mongodb()
