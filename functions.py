import pandas as pd
from sqlalchemy import create_engine
import os 
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

def extract_data():
    df = pd.read_csv(AIRFLOW_HOME + 'dags/odhf_bdoes_v1.csv',encoding='unicode_escape')
    df.dropna(subset=['street_name','source_format_str_address','CSDuid','latitude','longitude'], inplace=True)
    df['index'] = range(len(df))
    return df



def load_data(df):
    engine = create_engine('postgresql:import_csv//@localhost:5432/staging_hospital_data')
    df.to_sql("filtered_data_set",con=engine,if_exists='replace')