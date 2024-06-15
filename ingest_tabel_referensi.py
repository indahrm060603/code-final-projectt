from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    'ingest_tabel_referensi',
    default_args=default_args,
    description='Ingest data from PostgreSQL to Spark',
    schedule=timedelta(days=1),
)

output_path = '/home/nandaap/output'

def fetch_data_from_postgres(bank_customer_data, output_path):
    pg_hook = PostgresHook(postgres_conn_id='postgresnandaapconn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM bank_customer_data')
    result = cursor.fetchall()
    
    # Simpan hasil ke file CSV atau ke format lain yang diperlukan oleh Spark
    with open(output_path, 'w') as f:
        for row in result:
            f.write(','.join([str(item) for item in row]) + '\n')

fetch_referensi_task = PythonOperator(
    task_id='fetch_customer_from_postgres',
    python_callable=fetch_data_from_postgres,
    op_args=['bank_customer_data', '/home/nandaap/customer.csv'],
    dag=dag
)

spark_job = SparkSubmitOperator(
    task_id='run_spark_job',
    application='/home/nandaap/airflow/dags/spark_job_referensi.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '/home/nandaap/customer.csv', 
        'output',
        'jdbc:postgresql://localhost:5432/finalproject',
        'postgressnandaapconn',
        'nandaap10'
    ],
    dag=dag
)

fetch_referensi_task >> spark_job