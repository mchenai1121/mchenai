import pandas as pd
import tabula
import sqlalchemy
from datetime import datetime

frames = tabula.read_pdf("https://github.com/mchenai1121/mchenai/raw/main/con_9Dec22.pdf", lattice = True, pages = "all")

temp = frames[0]
column_names = temp.iloc[0]

for i in frames:
    i.columns = range(i.shape[1])
    i.drop(0, axis=0, inplace = True)

df = pd.concat(frames, ignore_index = True)

df.columns = list(column_names)

index_constituents = df[["Trade Date", "Stock\rName", "Closing\rPrice", "Weighting\r(%)"]]

index_constituents.rename(columns={"Trade Date":"date", 
                               "Stock\rName":"index_name", 
                               "Closing\rPrice":"ticker", 
                               "Weighting\r(%)":"weighting"},
                                inplace = True)

d = index_constituents["date"][0]
d_date = datetime.strptime(d, '%Y%m%d').date()
index_constituents["date"].replace(d, d_date, inplace = True)



conn_string = "postgresql://postgres:Leonhart0322@localhost:5432/postgres"

engine = sqlalchemy.create_engine(conn_string)


index_constituents.to_sql("index_constituents", engine, if_exists = "append", schema = "market_data", index = False)
