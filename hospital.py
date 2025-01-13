from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from functions import extract_data,transform_data,load_data

with DAG("import_hospital_data",start_date=datetime(2021,1,1),schedule_interval="@daily",catchup=False) as dag: 
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    # Set task dependencies
    extract >> transform >> load