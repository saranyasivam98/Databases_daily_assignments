import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import DATETIME
conn = "mysql+pymysql://saran:SADA2028jaya@localhost/yield_prediction"
engine = create_engine(conn, echo=True)

df = pd.read_excel("yield_files/testing.xlsx")
df.to_sql('testing', con=engine, if_exists='replace', index_label='id', dtype={"Date": DATETIME()})
