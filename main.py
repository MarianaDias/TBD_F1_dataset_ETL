import pandas as pd

import db_load
import transformation


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
    # Extract
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

    # Transformations
    championship_data = transformation.create_championship_aggregate(races_df, constructors_df, constructors_results_df,
                                                                     drivers_df,results_df)
    races_data = transformation.create_circuit_aggregate(circuits_df, races_df, results_df,drivers_df)
    lap_time_data = transformation.create_lap_time_aggregate(lap_time_df, drivers_df)

    # Load
    db_load.init_neo_load(drivers_df, constructors_df, races_df, results_df)
    db_load.init_mongo_load(races_data, championship_data, lap_time_data)

    print("ETL Completo")


if __name__ == '__main__':
    main()
