import pytest
from source.extraction import get_hourly_weather

def test_single_day():
    dataframe = get_hourly_weather(1, 19.4326, -99.1332, "2025-01-01")
    assert dataframe is not None
    print(dataframe)

test_single_day()