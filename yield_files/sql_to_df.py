import pandas as pd
from sqlalchemy import create_engine

conn = "mysql+pymysql://saran:SADA2028jaya@localhost/yield_prediction"
engine = create_engine(conn, echo=True)

query_soil = '''
select id, District as district, Zinc as zinc, Iron as iron, Manganese as manganese, Copper as copper from soil_nutrients
'''
query_ph = '''
select id, District as district, Year as year from ph_values
'''
query_gwl = '''
select id, District as district, Year as year, Season as season from groundwater_level
'''
query_yield = '''
select id, District as district, Year as year, Season as season, Crop as crop from yield_data
'''
query_acr = '''
select id, District as district, Year as year, Season as season, Crop as crop, actual_area from acreage
'''
soil = pd.read_sql_query(query_soil, engine)
ph = pd.read_sql_query(query_ph, engine)
gwl = pd.read_sql_query(query_gwl, engine)
yield_df = pd.read_sql_query(query_yield, engine)
acreage = pd.read_sql_query(query_acr, engine)

ph_soil = ph.merge(soil, how='outer', on="district")
ph_soil_gw = ph_soil.merge(gwl, how='outer', on=["district", "year"])
yield_psg = yield_df.merge(ph_soil_gw, how='left', on=['season', 'district', 'year'])
yield_apsg = yield_psg.merge(acreage, how='outer', on=['season', 'year', 'district', 'crop'])
print(yield_apsg.shape)

average_df = pd.read_csv("average_climate.csv")
yield_aapsg = yield_apsg.merge(average_df, how='left', on=['season', 'year', 'district'])


yield_aapsg.to_csv("regression_1.csv")


'''df = pd.read_sql_query("select id, crop_season as season, Date as date, Temp_min as temp_min, Temp_max as temp_max, Rainfall as rainfall, Humidity_max as humidity_max, Humidity_min as humidity_min, Wind_max as wind_max from weather ", parse_dates=['Date'], con=engine)
print(df.head())'''
