import pandas as pd


def complete_drivers_code(drivers_df):
    df = drivers_df.fillna(value={'code': drivers_df['surname'].str[0:3]})
    df['code'] = df['code'].str.upper()
    return df


def create_driver_fullname_col(drivers_df):
    drivers_df["fullname"] = drivers_df["forename"] + " " + drivers_df["surname"]
    return drivers_df


def create_laptime_seconds(lap_time_df):
    lap_time_df['seconds'] = lap_time_df['milliseconds'] / 1000
    return lap_time_df

def create_championship_aggregate(races_df, constructors_df, constructors_results_df, drivers_df, results_df):
    # print(list(races_df))
    # print(list(constructors_df))
    # print(list(constructors_results_df))
    # print(list(drivers_df))
    # print(list(results_df))

    # Merged Constructor DF
    merged_constructor_df = pd.merge(races_df, constructors_results_df, on='raceId')[["year", "constructorId", "points", "round"]]
    merged_constructor_df = pd.merge(merged_constructor_df, constructors_df, on='constructorId')[["year", "constructorRef", "points", "round"]]

    merged_driver_df = pd.merge(races_df, results_df, on='raceId')
    print(list(merged_driver_df))

