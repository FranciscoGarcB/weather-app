import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from extraction import get_hourly_weather

load_dotenv()

def get_connection():
    """
    Creates and return a Postgresql database connection.
    """
    conn = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )
    conn.autocommit = False
    return conn


def load_to_db(conn, dataframe: pd.DataFrame) -> None:
    """
    Inserts a pandas DataFrame to the Postgresql dabatabse en la tabla raw_weather
    """

    if dataframe.empty:
        print("Empty DataFrame, nothing to insert.")
    
    query: str = """
        INSERT INTO raw_weather 
        (city_id, datetime, temperature_celsius, humidity, precipitation, windspeed)
        VALUES %s
        ON CONFLICT (city_id, datetime) DO UPDATE;
    """

    records = dataframe.to_records(index=False)
    values = [(r.city_id, r.datetime, r.temperature_celsius, r.humidity, r.precipitation, r.windspeed) for r in records]

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()
        print(f"Data inserted succesfully.")


if __name__ == "__main__":
    start_date = "2025-02-01"
    end_date = "2025-08-31"

    conn = get_connection()
    query = "SELECT city_id, latitude, longitude FROM cities;"
    cities = pd.read_sql_query(query, conn)
    
    df_final = pd.DataFrame()
    for index, row in cities.iterrows():
        id = int(row["city_id"])
        lat = row["latitude"]
        lon = row["longitude"]

        temp_df = get_hourly_weather(id, lat, lon, start_date, end_date)
        df_final = pd.concat([df_final, temp_df])
        print(f"{id}/{len(cities)} cities updated.")
