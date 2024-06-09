from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from yfinance_wrapper.py import stock_data, create_avro, load_bigquery
from airflow.models import Variable

GCS_KEY = Variable.get('GOOGLE_CLOUD_STORAGE_KEY_SECRET')
quotes = 'bbca.jk'
table_id = 'bbca_jk_table'

date = datetime.now()
date_str = date.strftime('%Y-%m-%d')
end_date = date + timedelta(days=1)
end_date_str = end_date.strftime('%Y-%m-%d')

default_args = {
    "start_date":datetime(2023, 7, 7),
    "retries":10,
    "retry_delay":timedelta(minutes=15)
}

with DAG('dag_stock_bbca', schedule_interval='59 16 * * *', default_args=default_args, catchup=False, tags=['yfinance_dag']) as dag:
    
    retrieve_data = PythonOperator(
        task_id = 'task_id_1',
        python_callable = stock_data,
        op_args = [GCS_KEY, quotes, date_str, end_date_str]
    )
    
    create_avro_file = PythonOperator(
        task_id = 'task_2_id',
        python_callable = create_avro,
        op_args = [GCS_KEY, quotes, date_str]
    )
    
    load_to_bigquery = PythonOperator(
        task_id = 'task_3_id',
        python_callable = load_bigquery,
        op_args = [GCS_KEY, table_id, quotes, date_str]
    )
    
    
    retrieve_data >> create_avro_file >> load_to_bigquery
