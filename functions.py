import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import os 
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

def extract_data():
    df = pd.read_csv(AIRFLOW_HOME + 'dags/odhf_bdoes_v1.csv',encoding='unicode_escape')
    df.dropna(subset=['street_name','source_format_str_address','CSDuid','latitude','longitude'], inplace=True)
    df['index'] = range(len(df))
    url = URL.create(
    drivername='redshift+redshift_connector', 
    host='default-workgroup.058264275627.us-east-1.redshift-serverless.amazonaws.com', 
    port=5439, 
    database='hospital_data', # Amazon Redshift database
    username= os.environ.get('redshift_user'), # Amazon Redshift username
    password= os.environ.get('redshift_password') # Amazon Redshift password
    )
    engine = sa.create_engine(url)
    table_name = 'hospital_data'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    index INT,
    facility_name VARCHAR(255),
    source_facility_type VARCHAR(100),
    odhf_facility_type VARCHAR(100),
    provider VARCHAR(255),
    unit VARCHAR(50),
    street_no VARCHAR(50),
    street_name VARCHAR(255),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    province VARCHAR(2),
    source_format_str_address VARCHAR(255),
    CSDname VARCHAR(100),
    CSDuid INT,
    Pruid INT,
    latitude FLOAT,
    longitude FLOAT
);
"""

    with  engine.connect() as connection:
        connection.execute(f"TRUNCATE TABLE {table_name}")
        connection.execute(create_table_query)
    df.to_sql(table_name, engine, if_exists='append', index=False)



def analyze_data():
    print("Data has been analyzed")