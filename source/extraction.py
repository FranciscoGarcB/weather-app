import os
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta


def get_hourly_weather(city_id: int, latitude: float, longitude:float, start_date: str, end_date: str = None) -> pd.DataFrame | None:
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

    # Data validation
    if not isinstance(city_id, int):
        raise ValueError("city_id must be a valid integer.")
    
    if not (-90 <= latitude <= 90):
        raise ValueError("Latitude must be between -90 and 90")
    
    if not (-180 <= longitude <= 180):
        raise ValueError("Longitude must be between -180 and 180.")
    
    try:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("❌ start_date must be in format 'YYYY-MM-DD'.")

    if end_date is None:
        end_date = start_date
    else:
        try:
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("❌ end_date must be in format 'YYYY-MM-DD'.")

        if end_date_obj < start_date_obj:
            raise ValueError("❌ end_date cannot be earlier than start_date.")

    # API call
    url: str = "https://archive-api.open-meteo.com/v1/archive"
    params: dict = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "auto"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
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
    else:
        print(f"Error {response.status_code} for {city_id}: {response.text}")
        return None
