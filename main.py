import pandas as pd

import db_load
import transformation
import mongodb_connection as mongo_conn


def load_csv_file(path):
    try:
        return pd.read_csv(path, encoding='ISO-8859-1')
    except:
        print("Não foi possível carregar o arquivo " + path)
        return pd.DataFrame()


def print_df(name, df):
    print(name)
    print(list(df))
    print(df)


def main():
    # Load all dataset into dataframes
    circuits_df = load_csv_file('f1_dataset/circuits.csv')
    drivers_df = load_csv_file('f1_dataset/drivers.csv')
    constructors_df = load_csv_file('f1_dataset/constructors.csv')
    constructors_results_df = load_csv_file('f1_dataset/constructorResults.csv')
    results_df = load_csv_file('f1_dataset/results.csv')
    races_df = load_csv_file('f1_dataset/races.csv')
    lap_time_df = load_csv_file('f1_dataset/lapTimes.csv')
    print("Arquivos carregados")

    # Transformations
    lap_time_df = transformation.create_laptime_seconds(lap_time_df)
    drivers_df = transformation.create_driver_fullname_col(drivers_df)
    drivers_df = transformation.complete_drivers_code(drivers_df)
    print('Transformações executadas')

    transformation.create_championship_aggregate(races_df, constructors_df, constructors_results_df, drivers_df,
                                                 results_df)
    transformation.create_race_aggregate(circuits_df, races_df, lap_time_df, results_df,
                                         drivers_df)

    db_load.init_neo_load(drivers_df, constructors_df, races_df, results_df)

    # MongoDB Test Connection
    # dbname = mongo_conn.get_database()
    # print(dbname)
    # mongo_conn.create_db_test_collection()
    # mongo_conn.find_all(dbname[mongo_conn.COLLECTION_TEST])


if __name__ == '__main__':
    main()
