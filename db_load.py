import pandas as pd
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
    for i in range(0, relation['year'].count()):
        neo_conn.create_driver_constructor_relation(relation.iloc[i])
    print("Relationships Created at Neo")
