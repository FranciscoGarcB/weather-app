import os
import uuid
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from extraction import get_hourly_weather

load_dotenv()

def get_connection():
    """
    Creates and return a Snowflake connection.
    """
    conn = snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_DB_USER"),
        password = os.getenv("SNOWFLAKE_DB_PASSWORD"),
        account = os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database = os.getenv("SNOWFLAKE_DB"),
        schema = os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn


def load_to_snowflake(conn, dataframe: pd.DataFrame) -> None:
    """
    Inserts a pandas DataFrame to the Snowflake dabatabse 
    """

    if dataframe.empty:
        print("Empty DataFrame, nothing to insert.")
    
    temp_table = f"TMP_RAW_WEATHER_{uuid.uuid4().hex[:6].upper()}" 

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} AS SELECT * FROM RAW_WEATHER WHERE 1=0;
        """)
    
        insert_query = f"""
            INSERT INTO {temp_table} (CITY_ID, DATE_TIME, TEMPERATURE_CELSIUS, HUMIDITY, PRECIPITATION, WINDSPEED)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        data = [
                (
                    int(r.city_id),
                    r.datetime,
                    float(r.temperature_celsius),
                    float(r.humidity),
                    float(r.precipitation),
                    float(r.windspeed)
                )
                for r in dataframe.itertuples(index=False)
            ]
        
        cur.executemany(insert_query, data)
        print(f"Inserted {len(data)} rows into {temp_table}")

        # Massive merge
        merge_query = f"""
            MERGE INTO RAW_WEATHER AS target
            USING {temp_table} AS src
            ON target.CITY_ID = src.CITY_ID AND target.DATE_TIME = src.DATE_TIME
            WHEN NOT MATCHED THEN INSERT 
                (CITY_ID, DATE_TIME, TEMPERATURE_CELSIUS, HUMIDITY, PRECIPITATION, WINDSPEED)
            VALUES 
                (src.CITY_ID, src.DATE_TIME, src.TEMPERATURE_CELSIUS, src.HUMIDITY, src.PRECIPITATION, src.WINDSPEED);
            """
        cur.execute(merge_query)
        conn.commit()
        print("Merge completed successfully.")

if __name__ == "__main__":
    start_date = datetime.now() - timedelta(days=3)
    start_date = start_date.strftime("%Y-%m-%d")
    start_date = "2025-01-01"
    end_date = datetime.now() - timedelta(days=1)
    end_date = end_date.strftime("%Y-%m-%d")

    print(f"Updating database from {start_date} to {end_date}.")

    conn = get_connection()
    query = "SELECT CITY_ID, LATITUDE, LONGITUDE FROM cities;"
    cities = pd.read_sql_query(query, conn)
    
    for index, row in cities.iterrows():
        id = int(row["CITY_ID"])
        lat = row["LATITUDE"]
        lon = row["LONGITUDE"]

        temp_df = get_hourly_weather(id, lat, lon, start_date, end_date)

        load_to_snowflake(conn=conn, dataframe=temp_df)
        print(f"{id}/{len(cities)} cities updated.")

    conn.close()
