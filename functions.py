import pandas as pd
import sqlalchemy
from datetime import datetime
import tabula
import calendar

conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string).connect()

latest_url_si = "https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares/Latest-CSV"

date_url_si = "https://www.sfc.hk/-/media/EN/pdf/spr/{year}/{month:02d}/{day:02d}/Short_Position_Reporting_Aggregated_Data_{year}{month:02d}{day:02d}.csv"

date_url_idx = "https://www.hsi.com.hk/static/uploads/contents/en/indexes/report/hsi/con_{day}{month}{year}.pdf"

storage_options = {'User-Agent': 'Mozilla/5.0'}

def convert_and_insert_si(df, table, SCHEMA):
    
    short_interest = df.drop(columns=["Aggregated Reportable Short Positions (HK$)"])

    d = short_interest["Date"][0]
    d_date = datetime.strptime(d, '%d/%m/%Y').date()
    short_interest["Date"].replace(d, d_date, inplace = True)
    df = df.astype({'Stock Code': str})


    short_interest.rename(columns={"Date":"date", 
                                   "Stock Name":"stock_name", 
                                   "Aggregated Reportable Short Positions (Shares)":"short_interest", 
                                   "Stock Code":"ticker"},
                                    inplace = True)

    short_interest.to_sql(table, engine, if_exists = "append", schema = SCHEMA, index = False)


def get_historical_short_interest(date, table, SCHEMA):
    url = date_url_si.format(year=date.year, month=date.month, day=date.day)
    df = pd.read_csv(url, storage_options=storage_options)
    
    old_data = pd.read_sql_table(table, engine, schema = SCHEMA)
    
    d = datetime.strptime(df["Date"][0], '%d/%m/%Y').date()
    
    new_date = datetime(d.year, d.month, d.day)
    
    if new_date not in list(old_data["date"]):
        convert_and_insert_si(df, table, SCHEMA)
    

def check_date_si():
    new_data = pd.read_csv(latest_url_si, storage_options=storage_options)
    
    old_data = pd.read_sql_table('short_interest', engine, schema = 'market_data')
    
    d = datetime.strptime(new_data["Date"][0], '%d/%m/%Y').date()
    
    new_date = datetime(d.year, d.month, d.day)
    
    if new_date not in list(old_data["date"]):
        convert_and_insert_si(new_data, "short_interest", "market_data")
        

def convert_and_insert_idx(df, table, SCHEMA):
    df.rename(columns={"Trade Date":"date", 
                                   "Stock\rName":"index_name", 
                                   "Stock Code":"ticker", 
                                   "Weighting\r(%)":"weighting"},
                                    inplace = True)

    d = df["date"][0]
    d_date = datetime.strptime(d, '%Y%m%d').date()
    df["date"].replace(d, d_date, inplace = True)
    
    df.to_sql(table, engine, if_exists = "append", schema = SCHEMA, index = False)


def get_historical_idx(date, table, SCHEMA):
    url = date_url_idx.format(day = date.day, month = calendar.month_abbr[date.month], year = str(date.year)[2:])
    
    data = tabula.read_pdf(url, lattice = True, pages = "all")
    
    temp = data[0]
    column_names = temp.iloc[0]

    for i in data:
        i.columns = range(i.shape[1])
        i.drop(0, axis=0, inplace = True)

    df = pd.concat(data, ignore_index = True)

    df.columns = list(column_names)

    new_data = df[["Trade Date", "Stock\rName", "Stock Code", "Weighting\r(%)"]]
    
    old_data = pd.read_sql_table(table, engine, schema = SCHEMA)
    
    d = datetime.strptime(new_data["Trade Date"][0], '%Y%m%d').date()
    
    new_date = datetime(d.year, d.month, d.day)
    
    if new_date not in list(old_data["date"]):
        convert_and_insert_idx(new_data, table, SCHEMA)
    
