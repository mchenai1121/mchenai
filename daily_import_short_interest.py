from functions import check_date_si
import sqlalchemy


conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

if __name__ == "__main__":
    check_date_si()