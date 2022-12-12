import pandas as pd
import sqlalchemy
from datetime import datetime

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

url = "https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares/Latest-CSV"

storage_options = {'User-Agent': 'Mozilla/5.0'}

def convert_and_insert(df):
    
    short_interest = df.drop(columns=["Stock Code"])

    d = short_interest["Date"][0]
    d_date = datetime.strptime(d, '%d/%m/%Y').date()
    short_interest["Date"].replace(d, d_date, inplace = True)


    short_interest.rename(columns={"Date":"date", 
                                   "Stock Name":"stock_name", 
                                   "Aggregated Reportable Short Positions (Shares)":"short_interest", 
                                   "Aggregated Reportable Short Positions (HK$)":"ticker"},
                                    inplace = True)

    short_interest.to_sql("short_interest", engine, if_exists = "append", schema = "market_data", index = False)

def check_date():
    new_data = pd.read_csv(url, storage_options=storage_options)
    
    old_data = pd.read_sql_table('short_interest', engine, schema = 'market_data')
    
    new_date = datetime.strptime(new_data["Date"][0], '%d/%m/%Y').date()
    
    if new_date not in old_data["date"]:
        convert_and_insert(new_data)