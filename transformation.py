import pandas as pd


def complete_drivers_code(drivers_df):
    df = drivers_df.fillna(value={'code': drivers_df['surname'].str[0:3]})
    df['code'] = df['code'].str.upper()
    df["code"] = drivers_df["code"].str.replace("'", "/")
    return df


def create_driver_fullname_col(drivers_df):
    drivers_df["fullname"] = drivers_df["forename"] + " " + drivers_df["surname"]
    drivers_df["fullname"] = drivers_df["fullname"].str.replace("'"," ")
    return drivers_df


def create_laptime_seconds(lap_time_df):
    lap_time_df['seconds'] = lap_time_df['milliseconds'] / 1000
    return lap_time_df


def create_championship_aggregate(races_df, constructors_df, constructors_results_df, drivers_df, results_df):
    championship = []

    # Merged Constructor DF
    merged_constructor_df = pd.merge(races_df, constructors_results_df, on='raceId')[
        ["year", "constructorId", "points", "round"]]
    merged_constructor_df = pd.merge(merged_constructor_df, constructors_df, on='constructorId')[
        ["year", "constructorRef", "points", "round"]]

    # Merged Driver DF
    merged_driver_df = pd.merge(races_df, results_df, on='raceId')[["year", "driverId", "round", "points"]]
    merged_driver_df = pd.merge(merged_driver_df, drivers_df, on='driverId')[['year', 'driverRef', 'round', 'points']]

    for year in merged_constructor_df["year"].unique():
        constructors_year_df = merged_constructor_df.loc[merged_constructor_df['year'] == year]
        drivers_year_df = merged_driver_df.loc[merged_driver_df['year'] == year]
        championship_item = {"year": year, "constructorResults": constructors_year_df.drop(columns=['year']).to_dict('records'),
                             "driverResults": drivers_year_df.drop(columns=['year']).to_dict('records')}
        championship.append(championship_item)
    return championship


def create_race_aggregate(circuits_df, races_df, lap_time_df, results_df,  drivers_df):
    pd.set_option('display.max_columns', None)
    agg = []

    # Merged lap data
    merged_lap_df = pd.merge(lap_time_df, drivers_df, on='driverId')[['raceId', 'driverRef', 'lap', 'position', 'seconds']]

    # Merged circuit and race data
    merged_race_circuit_df = pd.merge(races_df, circuits_df, on='circuitId')
    merged_race_circuit_df = merged_race_circuit_df.rename(columns={"name_x": "raceName"})
    merged_race_circuit_df = merged_race_circuit_df[['raceId', 'year', 'round', 'raceName', 'circuitId', 'date']]

    for circuit_id in merged_race_circuit_df['circuitId'].unique():
        df = merged_race_circuit_df.loc[merged_race_circuit_df['circuitId'] == circuit_id]
        c = circuits_df.loc[circuits_df['circuitId'] == circuit_id]

        #todo: adicionar tempos de volta
        #todo: adicionar results

        circuit_item = {
            "name": c['name'].iloc[0],
            "location": {"city": c['location'].iloc[0], "country": c['country'].iloc[0]},
            "races": df.drop(columns="circuitId").to_dict('records')
        }
        agg.append(circuit_item)
    print(agg[0])
    return agg