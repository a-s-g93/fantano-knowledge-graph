from neo4j import GraphDatabase

def init_driver(uri, username, password):
    """
        Initiate the Neo4j Driver
    """
    d = GraphDatabase.driver(uri, auth=(username, password))
    d.verify_connectivity()
    print('driver created')
    return d