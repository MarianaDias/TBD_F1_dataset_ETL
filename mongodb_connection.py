from pymongo import MongoClient

MONGO_CONNECTION_STR = "mongodb://localhost:27017/TBD_G14"
COLLECTION_TEST = "f1_test_collection"


def get_database(connection_string, collection_name):
    client = MongoClient(connection_string)
    return client[collection_name]


def create_db_collection(name, items, dbname):
    collection_name = dbname[name]
    collection_name.insert_many(items)


def find_all(collection_name):
    item_details = collection_name.find()
    for item in item_details:
        print(item)


def create_db_test_collection():
    item_1 = {
        "circuit_name": "Circuit de Monaco",
        "location": [{"city": "Monte-Carlo", "country": "Monaco"}]
    }

    item_2 = {
        "circuit_name": "Hungaroring",
        "location": [{"city": "Budapest", "country": "Hungary"}]
    }

    create_db_collection("f1_test_collection", [item_1, item_2], get_database(MONGO_CONNECTION_STR, COLLECTION_TEST))
    print("Test items added")
