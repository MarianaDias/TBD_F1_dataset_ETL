
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
neo_driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def create_driver_constructor_relation(driver_name, constructor_name):
    query = (
            "CREATE (d:Driver { name: '" + driver_name + "' }) "
            "CREATE (c:Constructor { name: '" + constructor_name + "' }) "
            "CREATE (d)-[:DRIVEN_FOR]->(c) "
    )
    with neo_driver.session() as session:
        session.run(query)

def find_all():
    query = "MATCH p=()-[r:DRIVEN_FOR]->() RETURN p"
    with neo_driver.session() as session:
        result = session.run(query)
        print(result.values())

def close():
    neo_driver.close()