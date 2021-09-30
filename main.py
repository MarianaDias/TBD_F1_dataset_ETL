import pandas as pd
import tranformation

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

    #Transformations
    tranformation.create_laptime_seconds(lap_time_df)
    tranformation.create_driver_fullname_col(drivers_df)
    tranformation.complete_drivers_code(drivers_df)

if __name__ == '__main__':
    main()
