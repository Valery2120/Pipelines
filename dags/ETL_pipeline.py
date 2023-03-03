from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os
import requests


default_args = {
    "retries": 3,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='weather_ETL',
     default_args=default_args,
     start_date=datetime(2022, 12, 9),
     schedule_interval=timedelta(minutes=1440)
     )
def etl():

    @task()
    def extract_data() -> dict:
        """
        Query openweathermap.com's API and to get the weather for
        Gomel, BY and then dump the json to the /src/data/ directory
        with the file name "<today's date>.json"
        """

        load_dotenv()
        API_KEY = os.getenv('API_KEY')
        url = os.getenv('url')

        paramaters = {'q': 'Gomel, BY', 'appid': API_KEY}
        result = requests.get(url, paramaters)

        if result.status_code == 200:
            json_data = result.json()
            return json_data
        else:
            print("Error In API call.")

    @task()
    def transform_data(data: dict) -> tuple:
        """
        Processes the json data and change the types.
        """

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
        print(row)
        return row

    @task
    def load_data(data: tuple) -> None:
        """
        Load data into the Postgres database
        """

        pg_hook = PostgresHook(postgres_conn_id='weather_table')

        insert_data = """INSERT INTO weather_table
                            (city, country, latitude, longitude,
                             humidity, pressure, temperature, weather, date)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        pg_hook.run(insert_data, parameters=data)

    extract = extract_data()
    transform = transform_data(data=extract)
    load_data(data=transform)


etl_dag = etl()
