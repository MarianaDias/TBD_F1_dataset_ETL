from pymongo import MongoClient

MONGO_CONNECTION_STR = "mongodb://localhost:27017/"
DB_TEST = "F1"
COLLECTION_TEST = "f1_collection"


def get_database():
    client = MongoClient(MONGO_CONNECTION_STR)
    return client[DB_TEST]


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

    create_db_collection(COLLECTION_TEST, [item_1, item_2], get_database())
    print("Test items added")


def create_agg(items, message, collection):
    create_db_collection(collection, items, get_database())
    print(message)
