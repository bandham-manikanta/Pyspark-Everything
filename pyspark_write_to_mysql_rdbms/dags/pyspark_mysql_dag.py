from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'pyspark_job',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='*/2 * * * *',
    start_date=datetime(2023, 7, 23),
    catchup=False,
) as dag:

    # Define the BashOperator task
    spark_workflow_task = BashOperator(
        task_id='pyspark_job_task',
        bash_command='spark-submit --jars /home/bandham/airflow/mysql-connector-j-8.2.0.jar /home/bandham/airflow/pyspark_mysql_write.py',
        dag=dag
    )

    spark_workflow_task