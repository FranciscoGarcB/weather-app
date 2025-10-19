import pytest
from dotenv import load_dotenv
from source.loading import load_to_db, get_connection

conn = get_connection()
print(conn)