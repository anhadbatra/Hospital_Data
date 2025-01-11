from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime
from sqlalchemy import create_engine



def extract_data():
    df = pd.read_csv('odhf_bdoes_v1.csv',encoding='unicode_escape')
    return df

def transform_data(df):
    df.dropna(subset=['street_name','source_format_str_address','CSDuid','latitude','longitude'], inplace=True)
    df['index'] = range(len(df))
    return df

def load_data(df):
    engine = create_engine('postgresql:import_csv//@localhost:5432/staging_hospital_data')
    df.to_sql("filtered_data_set",con=engine,if_exists='replace')

with DAG("import_hospital_data",start_date=datetime(2021,1,1),schedule_interval="@daily",catchup=False) as dag: 
    extract = PythonVirtualenvOperator(
        task_id="extract",
        python_callable=extract_data,
        requirements=["pandas","sqlalchemy"]
        provide_context=False,
    )

    transform = PythonVirtualenvOperator(
        task_id="transform",
        python_callable=transform_data
        requirements=["pandas"],
        provide_context=False,
    )

    load = PythonVirtualenvOperator(
        task_id="load",
        python_callable=load_data
        requirements=["pandas","sqlalchemy"],
        provide_context=False,
    )

    extract >> transform >> load