from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("meteo_pipeline", start_date=datetime(2024,1,1), schedule_interval="@daily") as dag:

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command="python /opt/airflow/producer.py"
    )

    start_spark = BashOperator(
        task_id="start_spark",
        bash_command="/opt/spark/bin/spark-submit /opt/airflow/streaming.py"
    )

    start_producer >> start_spark