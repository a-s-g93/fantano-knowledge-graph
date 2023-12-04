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
        
        if not driver:
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

    def upload_transcripts(self, data: List[Dict[str, str]]) -> None:
        """
        This method uploads the video transcripts into the graph.
        """

        def run(tx):
            tx.run(
                """
                unwind $data as row

                merge (v:Video {id: row.video_id,
                               address: row.video_address,
                               title: row.title})
                merge (d:Document {text: row.transcript})
                merge (v)-[:HAS_DOCUMENT]->(d)
                
                """, data=data
            )
   
        try:
            with self.driver.session(database=self.database_name) as session:
                print("database name: ", self.database_name)
                session.execute_write(run)
            
        except ConstraintError as err:
            print(err)

            session.close()
