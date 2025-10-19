import pytest
from source.extraction import get_hourly_weather

def test_single_day():
    dataframe = get_hourly_weather(1, 19.4326, -99.1332, "2025-01-01")
    assert dataframe is not None
    print(dataframe)

def test_multiple_days():
    dataframe = get_hourly_weather(1, 9.4326, -99.1332, "2025-01-01", "2025-01-05")
    assert len(dataframe["datetime"])==120