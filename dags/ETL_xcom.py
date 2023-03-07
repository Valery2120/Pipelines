from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os
import requests


default_args = {
    "retries": 3,
    'retry_delay': timedelta(minutes=1)
}


def extract_data(**kwargs) -> dict:
    """
    Query openweathermap.com's API and to get the weather for
    Gomel, BY and then dump the json to the /src/data/ directory
    with the file name "<today's date>.json"
    """


    ti = kwargs['ti']
    load_dotenv()
    API_KEY = os.getenv('API_KEY')
    url = os.getenv('url')

    paramaters = {'q': 'Gomel, BY', 'appid': API_KEY}
    result = requests.get(url, paramaters)

    if result.status_code == 200:
        json_data = result.json()
    else:
        print("Error In API call.")

    ti.xcom_push(key='weather_json', value=json_data)


def transform_data(**kwargs) -> tuple:
    """
    Processes the json data and change the types.
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(key='weather_json', task_ids=['extract_data'])[0]

    city = str(data['name'])
    country = str(data['sys']['country'])
    latitude = float(data['coord']['lat'])
    longitude = float(data['coord']['lon'])
    humidity = float(data['main']['humidity'])
    pressure = float(data['main']['pressure'])
    temperature = float(data['main']['temp']) - 273.15
    weather = str(data['weather'][0]['description'])
    date = str(datetime.now().date())
    row = (city, country, latitude, longitude, humidity, pressure, temperature, weather, date)

    ti.xcom_push(key='weather_row', value=row)


def load_data(**kwargs) -> None:
    """
    Load data into the Postgres database
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(key='weather_row', task_ids=['transform_data'])[0]

    pg_hook = PostgresHook(postgres_conn_id='weather_table')

    insert_data = """INSERT INTO weather_table
                        (city, country, latitude, longitude,
                         humidity, pressure, temperature, weather, date)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    pg_hook.run(insert_data, parameters=data)


dag =  DAG(dag_id='ETL_weather',
     default_args=default_args,
     start_date=datetime(2022, 12, 9),
     schedule_interval=timedelta(minutes=1440)
     )

extract = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag)

transform = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform_data,
    dag=dag)

load = PythonOperator(
    task_id='load_data',
    provide_context=True,
    python_callable=load_data,
    dag=dag)

extract >> transform >> load