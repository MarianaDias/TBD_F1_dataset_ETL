from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError, ClientError

uri = "neo4j://localhost:7687"
neo_driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


def create_driver_constructor_relation_test(driver_name, constructor_name):
    query = "CREATE (d:Driver { name: '%s' }) CREATE (c:Constructor { name: '%s' }) CREATE (d)-[:DRIVEN_FOR]->(c)" \
            % (driver_name, constructor_name)

    with neo_driver.session() as session:
        session.run(query)


def create_driver_constructor_relation(driver, constructor, years):
    query = "MATCH (d:Driver {ref:'%s'}),(c:Constructor {ref:'%s'}) CREATE ((d)-[r:DRIVEN_FOR {year: %s}]->(c))" % \
            (driver, constructor, years)
    with neo_driver.session() as session:
        session.run(query)


def create_constructor(constructor):
    query = "CREATE(d: Constructor { name: '%s' , ref: '%s', nationality: '%s' })" % (constructor["name"],
                                                                                      constructor["constructorRef"],
                                                                                      constructor["nationality"])
    with neo_driver.session() as session:
        try:
            session.run(query)
        except ConstraintError:
            print("Constructor already exists {0}".format(constructor["constructorRef"]))


def create_driver(driver):
    query = "CREATE(d: Driver { name: '%s' ,code: '%s', " \
            "ref: '%s', dob: '%s', nationality: '%s' })" % (driver["fullname"], driver["code"], driver["driverRef"],
                                                            driver["dob"], driver["nationality"])
    with neo_driver.session() as session:
        try:
            session.run(query)
        except ConstraintError:
            print("Driver already exists {0}".format(driver["driverRef"]))


def create_neo_constrains():
    constrain_constructor = "CREATE CONSTRAINT ON (c:Constructor) ASSERT c.ref IS UNIQUE"
    constrain_driver = "CREATE CONSTRAINT ON (c:Driver) ASSERT c.ref IS UNIQUE"
    with neo_driver.session() as session:
        try:
            session.run(constrain_constructor)
            session.run(constrain_driver)
        except ClientError:
            print('Constrains already created')


def find_all():
    query = "MATCH p=()-[r:DRIVEN_FOR]->() RETURN p"
    with neo_driver.session() as session:
        result = session.run(query)
        print(result.values())


def close():
    neo_driver.close()
