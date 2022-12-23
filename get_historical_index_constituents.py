import sqlalchemy
from datetime import datetime
from functions import get_historical_idx

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

date = datetime(2022, 12, 23)

get_historical_idx(date, "index_constituents", "market_data")