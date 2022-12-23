import sqlalchemy
from datetime import date
from functions import get_historical_idx

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

date = date.today()

if __name__ == "__main__":
    get_historical_idx(date, "index_constituents", "market_data")
