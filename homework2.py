import datetime
import time
import json
import requests
import pandas as pd 
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 10, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

folder = os.path.expanduser('-')+'/data_dags/'
json_name_file = 'data_json.json'
csv_name_file = 'data_csv.csv'
key_api = Variable.get('KEY_API')
city = 'Penza'
timezone = 3

def extract_data():
    response = requests.get(
        'http://api.worldweatheronline.com/premium/v1/weather.ashx',
        params={
            'q': '{}'.format(city),
            'format': 'json',
            'FX': 'no',
            'num_of_days': 1,
            'key': key_api,
            'includelocation': 'no'
        },
        headers={
            'Authorization': key_api
        }
    )

    if response.status_code == 200:
        json_data = response.json()
        with open(folder + json_name_file, 'w') as f:
            json.dump(json_data,f,indent=2)
            f.close()


def transform_data():
    with open(folder+json_name_file, 'r') as jd:
        json_data = json.load(jd)
        print(json_data)
        jd.close()

    value_list = []

    start_utc = datetime.datetime.utcnow()
    start_moscow = start_utc + datetime.timedelta(hours=moscow_timezone)

    city = json_data['data']['request'][0]['query']
    observation_time = json_data['data']['current_condition'][0]['observation_time']
    temp = json_data['data']['current_condition'][0]['temp_C']
    humidity = json_data['data']['current_condition'][0]['humidity']
    value_list.append(pd.to_datetime(observation_time).strftime('%Y-%m-%d %H:%M:%S'))
    res_df = pd.DataFrame(value_list,columns=['observation_time'])
    res_df["date_from_msk"] = start_moscow
    res_df["date_from_msk"] = pd.to_datetime(res_df["date_from_msk"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    res_df["date_from_utc"] = start_utc
    res_df["date_from_utc"] = pd.to_datetime(res_df["date_from_utc"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    res_df["city_observation"] = city
    res_df["city_temp_c"] = temp
    res_df["city_humidity"] = humidity
    print(res_df.head())
    with open(folder+csv_name_file, 'w') as f:
        res_df.to_csv(f, index=False)
        f.close()

def load_data():
    with open(folder+csv_name_file, 'r') as f:
        res_df=pd.read_csv(f, index_col=None)
        f.close()
    print(res_df.head())
with DAG(
        dag_id='weather_worldweatheronline_api',  
        schedule_interval='@once',  
        default_args=args, 
) as dag:

    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)

    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)

    load_data = PythonOperator(task_id='load_data', python_callable=load_data)

    extract_data >> transform_data >> load_data
