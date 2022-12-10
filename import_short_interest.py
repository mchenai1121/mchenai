import pandas as pd
import sqlalchemy


url = "https://github.com/mchenai1121/mchenai/raw/main/Short_Position_Reporting_Aggregated_Data_20221202.csv"
df = pd.read_csv(url)

short_interest = df.drop(columns=["Stock Code"])

short_interest.rename(columns={"Date":"date", 
                               "Stock Name":"stock_name", 
                               "Aggregated Reportable Short Positions (Shares)":"short_interest", 
                               "Aggregated Reportable Short Positions (HK$)":"ticker"},
                                inplace = True)

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string)


short_interest.to_sql("short_interest", engine, if_exists = "append", schema = "market_data", index = False)

