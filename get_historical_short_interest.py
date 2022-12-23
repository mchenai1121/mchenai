import sqlalchemy
from datetime import datetime
from functions import get_historical_short_interest

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

date = datetime(2022, 12, 9)

get_historical_short_interest(date, "short_interest", "market_data")