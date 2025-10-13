import os
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta


def get_hourly_weather(city_id: int, lat: float, lon:float, start_date: str, end_date: str = None):
    """
    Extracts hourly weather data from the Open-Meteo API for a specific city and date range.

    If no end_date is provided, the function retrieves data only for start_date.

    Args:
        city_id (str): Unique identifier of the city in the local database.
        lat (float): Latitude of the city.
        lon (float): Longitude of the city.
        start_date (str): Start date for data retrieval (format: 'YYYY-MM-DD').
        end_date (str, optional): End date for data retrieval. Defaults to start_date if not provided.

    Returns:
        pd.DataFrame | None: Response from the Open-Meteo API containing hourly data in a pandas DataFrame format,
                     or None if the request fails.

    Example:
        >>> get_hourly_weather(1, 19.4326, -99.1332, "2025-01-01", "2025-01-07")
    """

    if end_date is None:
        end_date = start_date

    url: str = "https://archive-api.open-meteo.com/v1/archive"

    params: dict = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "auto"
    }

    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame({
        "city_id": city_id,
        "datetime": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "humidity": data["hourly"]["relative_humidity_2m"],
        "precipitation": data["hourly"]["temperature_2m"],
        "windspeed": data["hourly"]["wind_speed_10m"]
    })

    return df

print(get_hourly_weather(1, 19.4326, -99.1332, "2025-01-01", "2025-01-07"))