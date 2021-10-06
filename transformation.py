import pandas as pd

def complete_drivers_code(drivers_df):
    df = drivers_df.fillna(value = {'code': drivers_df['surname'].str[0:3] })
    df['code'] = df['code'].str.upper()
    print(df["code"])
    return df

def create_driver_fullname_col(drivers_df):
    drivers_df["fullname"] = drivers_df["forename"] + " " + drivers_df["surname"]
    print(drivers_df)
    return drivers_df

def create_laptime_seconds(lap_time_df):
    lap_time_df['seconds'] = lap_time_df['milliseconds'] / 1000
    print(lap_time_df['seconds'])
    return lap_time_df