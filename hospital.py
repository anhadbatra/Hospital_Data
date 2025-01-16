from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from functions import extract_data,redshift_data

with DAG("import_hospital_data",start_date=datetime(2021,1,1),schedule_interval="@daily",catchup=False) as dag: 
    extract = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_data,
    )
    redshift_data_load = PythonOperator(
        task_id="redshift",
        python_callable=redshift_data,
    )


    # Set task dependencies
    extract >> redshift_data_load