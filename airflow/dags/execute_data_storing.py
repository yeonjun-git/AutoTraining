from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

app_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'app')


with DAG(
    dag_id="dbsave_training",
    schedule="0 15 * * *",
    start_date=pendulum.datetime(2023, 5, 29, tz="Asia/Seoul"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["Workflow of Save for Crawling data"],
) as dag:
    run_start = EmptyOperator(
        task_id="start",
    )

    running_dbsave = BashOperator(
        task_id="data-saving",
        bash_command="python3 DataStoring.py",
        cwd=app_path
    )

    run_end = EmptyOperator(
        task_id="end",
    )

    run_start >> running_dbsave >> run_end

if __name__ == "__main__":
    dag.test()
    