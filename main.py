import pandas as pd
import tranformation
import mongodb_connection as mongo_conn
import neo4j_connection as neo_conn

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
    # circuits_df = load_csv_file('f1_dataset/circuits.csv')
    # drivers_df = load_csv_file('f1_dataset/drivers.csv')
    # constructors_df = load_csv_file('f1_dataset/constructors.csv')
    # constructors_results_df = load_csv_file('f1_dataset/constructorResults.csv')
    # results_df = load_csv_file('f1_dataset/results.csv')
    # races_df = load_csv_file('f1_dataset/races.csv')
    # lap_time_df = load_csv_file('f1_dataset/lapTimes.csv')
    #
    # #Transformations
    # tranformation.create_laptime_seconds(lap_time_df)
    # tranformation.create_driver_fullname_col(drivers_df)
    # tranformation.complete_drivers_code(drivers_df)

    #MongoDB Test Connection
    # dbname = mconn.get_database(mconn.MONGO_CONNECTION_STR, mconn.COLLECTION_TEST)
    # print(dbname)
    # mongo_conn.create_db_test_collection()
    # mongo_conn.find_all(dbname[mconn.COLLECTION_TEST])

    #Neo4J test connection
    # neo_conn.create_driver_constructor_relation("Max Verstappen", "Redbull")
    # neo_conn.find_all()
    # neo_conn.close()
    print("Descomente/Personalize os trechos que desejar executar")

if __name__ == '__main__':
    main()
