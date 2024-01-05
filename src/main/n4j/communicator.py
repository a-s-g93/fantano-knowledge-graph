# import time
from typing import List, Optional, Dict
import os

from neo4j.exceptions import ConstraintError
from neo4j import Driver

from n4j import drivers
# from credentials import credentials

class Communicator:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    This class contains methods necessary to interact with the Neo4j database.
    """

    def __init__(self, driver: Driver = None) -> None:
        
        if driver is None:
            self.driver = drivers.init_driver(os.environ.get("NEO4J_URI"), 
                                              username=os.environ.get("NEO4J_USERNAME"), 
                                              password=os.environ.get("NEO4J_PASSWORD"))
        else:
            self.driver = driver
        self.database_name = os.environ.get("NEO4J_DATABASE")


    def test_database_connection(self):
        """
            This function tests database connection.
        """

        def test(tx):
            return tx.run(
                """
                match (d:Document)
                return d
                limit 1
                """
            ).data()
   
        try:
            with self.driver.session(database=self.database_name) as session:
                print("database name: ", self.database_name)
                result = session.execute_read(test)
                print("result: ", result)
            
                return result
            
        except ConstraintError as err:
            print(err)

            session.close()

    def neo4j_vector_index_search(self, embeddings: List[float], k: int = 10) -> None:
        """
        This method runs vector similarity search on the document embeddings against the question embedding.
        """

        def run(tx):
            return tx.run("""
                        CALL db.index.vector.queryNodes('document-embeddings', toInteger($k), $questionEmbedding)
                        YIELD node AS vDocs, score
                        return vDocs.url as url, vDocs.text as text, vDocs.index as index
                        """, {'questionEmbedding': embeddings, 'k': k}
                        )
        
        try:
            with self.driver.session(database=self.database_name) as session:
                result = session.execute_read(run)
            
                return result
            
        except ConstraintError as err:
            print(err)

            session.close()

    def load_nodes(self, data: List[Dict[str, str]]) -> None:
        """
        This method uploads the formatted data into the graph.
        """

        def run(tx):
            tx.run(
               query = """
                UNWIND $data AS param

                MERGE (child:Document {index: param.child_index})
                MERGE (parent:Document {index: param.parent_index})
                MERGE (s:Source {url: param.url})
                SET
                    child:Child,
                    child.createTime = datetime(),
                    child.text = param.transcript,
                    child.embedding = param.embedding,
                    
                    parent:Parent,
                    parent.text = param.parent_transcript,

                    s.title = param.title,
                    s.playlist_id = param.playlist_id,
                    s.video_id = param.video_id,
                    s.publish_date = param.publish_date
                    
                MERGE (parent)-[:HAS_SOURCE]->(s)
                MERGE (child)-[:HAS_PARENT]->(parent)
                """, data=data
            )
   
        try:
            with self.driver.session(database=self.database_name) as session:
                print("database name: ", self.database_name)
                session.execute_write(run)
            
        except ConstraintError as err:
            print(err)

            session.close()

    def create_constraints(self) -> None:
        """
        Create the constraints. 
        This method should be run before any data is uploaded.
        """

        def source_constraint(tx):
            tx.run(
                """
                CREATE CONSTRAINT source_id FOR (s:Source) REQUIRE s.id IS UNIQUE
                ;
                """
            )
        def document_constraint(tx):
            tx.run(
                """
                CREATE CONSTRAINT document_id FOR (d:Document) REQUIRE d.id IS UNIQUE
                ;
                """
            )
   
        try:
            with self.driver.session(database=self.database_name) as session:
                print("database name: ", self.database_name)
                session.execute_write(source_constraint)
                session.execute_write(document_constraint)
            
        except ConstraintError as err:
            print(err)

            session.close()

    def create_indexes(self) -> None:
        """
        Create the indexes. 
        """

        def vector_index(tx):
            tx.run(
                """
                CREATE VECTOR INDEX `text-embeddings`
                FOR (n: Child) ON (n.embedding)
                OPTIONS {indexConfig: {
                `vector.dimensions`: 96,
                `vector.similarity_function`: 'cosine'
                }}
                ;
                """
            )
      
   
        try:
            with self.driver.session(database=self.database_name) as session:
                print("database name: ", self.database_name)
                session.execute_write(vector_index)
            
        except ConstraintError as err:
            print(err)

            session.close()
