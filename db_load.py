import pandas as pd

import mongodb_connection
import neo4j_connection as neo_conn


def init_neo_load(drivers_df, constructor_df, races_df, results_df):
    neo_conn.create_neo_constrains()
    load_drivers_neo(drivers_df)
    load_constructors(constructor_df)
    load_constructor_drivers_relation(constructor_df, drivers_df, races_df, results_df)


def load_drivers_neo(drivers_df):
    for id in drivers_df["driverId"].unique():
        driver = drivers_df.loc[drivers_df["driverId"] == id][["fullname", "dob", "nationality", "code", "driverRef"]]
        neo_conn.create_driver(driver.to_dict('records')[0])
    print("Drivers Loaded to Neo")


def load_constructors(constructors_df):
    for id in constructors_df["constructorId"].unique():
        constructor = constructors_df.loc[constructors_df["constructorId"] == id][
            ["name", "constructorRef", "nationality"]]
        neo_conn.create_constructor(constructor.to_dict('records')[0])
    print("Constructors Loaded to Neo")


def load_constructor_drivers_relation(constructor_df, driver_df, races_df, result_df):
    relation = result_df[['raceId', 'driverId', 'constructorId']]
    race_year = races_df[['raceId', 'year']]
    relation = pd.merge(relation, race_year, on='raceId')[['driverId', 'constructorId', 'year']].drop_duplicates()
    relation = pd.merge(relation, constructor_df, on='constructorId', how='left')[['driverId', 'constructorId',
                                                                                   'year', 'constructorRef']]
    relation = pd.merge(relation, driver_df, on='driverId')[['year', 'constructorRef', 'driverRef']]
    for d in relation['driverRef'].unique():
        df = relation.loc[relation['driverRef'] == d]
        for c in df['constructorRef'].unique():
            dfc = df.loc[relation['constructorRef'] == c]
            neo_conn.create_driver_constructor_relation(d,c,dfc['year'].to_list())
    print("Relationships Created at Neo")


def init_mongo_load(races, championship, lap_time):
    mongodb_connection.create_agg(races, 'Circuitos/Corridas carregadas no mongo', 'circuits')
    mongodb_connection.create_agg(championship, 'Campionatos carregados no mongo', 'championship')
    mongodb_connection.create_agg(lap_time, 'Tempos de volta carregados no mongo', 'lapTime')
