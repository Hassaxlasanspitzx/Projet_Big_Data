from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.fetch_data_from_google import fetch_data_from_google
from lib.fetch_data_from_youtube import fetch_data_from_youtube


with DAG(
       'DAG',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(minutes=1),
       },
       description='Project_DAG',
       schedule='0 0 * * *',
       start_date=datetime(2023, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:


   def F_google():
       print("Hello Airflow - This is Task ")

   def F_youtube():
       print("Hello Airflow - This is Task ")

   def combination():
       print("Hello Airflow - This is Task 4")
   def indexing():
       print("Hello Airflow - This is Task 5")

   t1 = PythonOperator(
       task_id='source_to_raw_1',
       python_callable=fetch_data_from_google,
       op_kwargs={'keyword': 'cosmetics'}

   )

   t2 = PythonOperator(
       task_id='source_to_raw_2',
       python_callable=fetch_data_from_youtube,
       op_kwargs={'query': 'cosmetics'}
   )

   t3 = PythonOperator(
       task_id='raw_to_formatted_1',
       python_callable=F_google
   )

   t4 = PythonOperator(
       task_id='raw_to_formatted_2',
       python_callable=F_youtube
   )

   t5 = PythonOperator(
       task_id='produce_usage',
       python_callable=combination
   )

   t6 = PythonOperator(
       task_id='index_to_elastic',
       python_callable=indexing
   )

t1 >> t3 >> t5 >> t6
t2 >> t4 >> t5 >> t6

