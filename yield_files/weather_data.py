# -*- coding: utf-8 -*-

import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


def create_kharif(df, year):
    kharif_1718 = df[(df.Date.dt.month >= 7) & (df.Date.dt.year == year) & (df.Date.dt.month <= 10)]

    dates = kharif_1718.Date.unique()
    districts = kharif_1718.District.unique()

    for date in dates:
        for district in districts:
            test1 = kharif_1718[(kharif_1718.Date == date) & (kharif_1718.District == district)]
            kharif_1718.loc[(kharif_1718["Date"] == date) & (kharif_1718["District"] == district), "Temp_min"] = test1[
                "Temp_min"].fillna(test1["Temp_min"].mean())
            kharif_1718.loc[(kharif_1718["Date"] == date) & (kharif_1718["District"] == district), "Temp_max"] = test1[
                "Temp_max"].fillna(test1["Temp_max"].mean())
            kharif_1718.loc[(kharif_1718["Date"] == date) & (kharif_1718["District"] == district), "Humidity_min"] = \
                test1["Humidity_min"].fillna(test1["Humidity_min"].mean())
            kharif_1718.loc[(kharif_1718["Date"] == date) & (kharif_1718["District"] == district), "Humidity_max"] = \
                test1["Humidity_max"].fillna(test1["Humidity_max"].mean())
            kharif_1718.loc[(kharif_1718["Date"] == date) & (kharif_1718["District"] == district), "Wind_max"] = test1[
                "Wind_max"].fillna(test1["Wind_max"].mean())
    return kharif_1718


def create_rabi_1718(df):
    rabi_1718 = df[(df.Date.dt.month > 10) & (df.Date.dt.year == 2017)]
    rabi_1718.head()

    rabi_2 = df[(df.Date.dt.month < 4) & (df.Date.dt.year == 2018)]

    rabi_1718 = rabi_1718.append(rabi_2, ignore_index=True)

    dates = rabi_1718.Date.unique()
    districts = rabi_1718.District.unique()

    for date in dates:
        for district in districts:
            test1 = rabi_1718[(rabi_1718.Date == date) & (rabi_1718.District == district)]
            rabi_1718.loc[(rabi_1718["Date"] == date) & (rabi_1718["District"] == district), "Temp_min"] = test1[
                "Temp_min"].fillna(test1["Temp_min"].mean())
            rabi_1718.loc[(rabi_1718["Date"] == date) & (rabi_1718["District"] == district), "Temp_max"] = test1[
                "Temp_max"].fillna(test1["Temp_max"].mean())
            rabi_1718.loc[(rabi_1718["Date"] == date) & (rabi_1718["District"] == district), "Humidity_min"] = test1[
                "Humidity_min"].fillna(test1["Humidity_min"].mean())
            rabi_1718.loc[(rabi_1718["Date"] == date) & (rabi_1718["District"] == district), "Humidity_max"] = test1[
                "Humidity_max"].fillna(test1["Humidity_max"].mean())
            rabi_1718.loc[(rabi_1718["Date"] == date) & (rabi_1718["District"] == district), "Wind_max"] = test1[
                "Wind_max"].fillna(test1["Wind_max"].mean())
    return rabi_1718


def create_rabi_1819(df1, df2):
    rabi_1819 = df1[(df1.Date.dt.month > 10) & (df1.Date.dt.year == 2018)]
    rabi_2 = df2[(df2.Date.dt.month < 4) & (df2.Date.dt.year == 2019)]

    rabi_1819 = rabi_1819.append(rabi_2, ignore_index=True)

    dates = rabi_1819.Date.unique()
    districts = rabi_1819.District.unique()

    for date in dates:
        for district in districts:
            test1 = rabi_1819[(rabi_1819.Date == date) & (rabi_1819.District == district)]
            rabi_1819.loc[(rabi_1819["Date"] == date) & (rabi_1819["District"] == district), "Temp_min"] = test1[
                "Temp_min"].fillna(test1["Temp_min"].mean())
            rabi_1819.loc[(rabi_1819["Date"] == date) & (rabi_1819["District"] == district), "Temp_max"] = test1[
                "Temp_max"].fillna(test1["Temp_max"].mean())
            rabi_1819.loc[(rabi_1819["Date"] == date) & (rabi_1819["District"] == district), "Humidity_min"] = test1[
                "Humidity_min"].fillna(test1["Humidity_min"].mean())
            rabi_1819.loc[(rabi_1819["Date"] == date) & (rabi_1819["District"] == district), "Humidity_max"] = test1[
                "Humidity_max"].fillna(test1["Humidity_max"].mean())
            rabi_1819.loc[(rabi_1819["Date"] == date) & (rabi_1819["District"] == district), "Wind_max"] = test1[
                "Wind_max"].fillna(test1["Wind_max"].mean())
    return rabi_1819


def main():
    df = pd.read_excel("6_weather_data_ (1).xlsx")
    df1 = pd.read_csv("January_to_March_2019.csv")

    df["Date"] = pd.to_datetime(df["Date"])
    df = df[["District", "crop_season", "Date", "Temp_min", "Temp_max", "Rainfall", "Humidity_min", "Humidity_max",
             "Wind_max"]]

    df1["Date"] = pd.to_datetime(df1["Date"])
    df1.rename(columns={df1.columns[3]: "Rainfall", df1.columns[4]: "Temp_min", df1.columns[5]: "Temp_max",
                        df1.columns[6]: "Humidity_min",
                        df1.columns[7]: "Humidity_max", df1.columns[9]: "Wind_max"}, inplace=True)
    # LOGGER.info("Column names after changing: %s" % df.columns)
    df1["crop_season"] = 'rabi'
    df1 = df1[["District", "crop_season", "Date", "Temp_min", "Temp_max", "Rainfall", "Humidity_min", "Humidity_max",
               "Wind_max"]]

    kharif_1718 = create_kharif(df, 2017)
    LOGGER.info("Shape of kharif_1718 data: %s %s" % kharif_1718.shape)

    kharif_1819 = create_kharif(df, 2018)
    LOGGER.info("Shape of kharif_1819 data: %s %s" % kharif_1819.shape)

    rabi_1718 = create_rabi_1718(df)
    LOGGER.info("Shape of rabi_1718 data: %s %s" % rabi_1718.shape)

    rabi_1819 = create_rabi_1819(df, df1)
    LOGGER.info("Shape of rabi_1718 data: %s %s" % rabi_1819.shape)

    # LOGGER.info("Months in 2018-2019: %s" % list(rabi_1819.Data.dt.month.unique()))

    climate_df = pd.concat([kharif_1718, rabi_1718, kharif_1819, rabi_1819], ignore_index=True)
    LOGGER.info("Shape of climate_df data: %s %s" % climate_df.shape)
    climate_df.to_csv("climate_values.csv")


if __name__ == '__main__':
    main()
