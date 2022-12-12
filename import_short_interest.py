import pandas as pd
import sqlalchemy
from functions import convert_and_insert


url_git = "https://github.com/mchenai1121/mchenai/raw/main/Short_Position_Reporting_Aggregated_Data_20221202.csv"

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string)

df = pd.read_csv(url_git)

convert_and_insert(df)

