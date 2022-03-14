import sqlite3
import requests
import json
from collections import namedtuple
from contextlib import closing
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jose.rubio5317@alumnos.udg.mx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size':10})
    response_json = json.loads(r.text)
    with open("./raw_data", 'w') as archivo:
        json.dump(response_json['hits']['hits'], archivo, indent=5)

def process_data():
    complaints = []
    Complaint = namedtuple('Complaint', ['data_received', 'state', 'product', 'company', 'complaint_what_happened'])
    with open("./raw_data", 'r') as archivo:
        raw_data = json.load(archivo)
        for row in raw_data:
            source = row.get('_source')
            this_complaint = Complaint(
                data_received=source.get('data_recieved'),
                state=source.get('state'),
                product=source.get('product'),
                company=source.get('company'),
                complaint_what_happened=source.get('complaint_what_happened')
            )
            complaints.append(this_complaint)
    with open("./parsed_data", 'w') as archivo:
            json.dump(complaints, archivo, indent=5)

def store_data():
    with open("./parsed_data", 'r') as archivo:
        complaints = json.load(archivo)
    
    create_script = 'CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, complaints)
            conn.commit()

with DAG(
    'act7_RAJ',
    default_args=default_args,
    description='Ejemplo de DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['ejemplo']
) as dag:
    get_data_task = PythonOperator(task_id="get_data", python_callable=get_data)
    process_data_task = PythonOperator(task_id="process_data", python_callable=process_data)
    store_data_task = PythonOperator(task_id="store_data", python_callable=store_data)

    get_data_task >> process_data_task >> store_data_task