# import time
from typing import List, Optional

from neo4j.exceptions import ConstraintError
from neo4j import Driver

# from langchain.chat_models import ChatVertexAI, AzureChatOpenAI
# from langchain.chains import ConversationChain
# from langchain.memory import ConversationSummaryBufferMemory
# from langchain.prompts.prompt import PromptTemplate

import drivers
from credentials import credentials

class DAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    This class contains methods necessary to interact with the Neo4j database.
    """

    def __init__(self, driver: Driver) -> None:
        
        if not driver:
            self.driver = drivers.init_driver(credentials['uri'], username=credentials['username'], password=credentials['password'])
        else:
            self.driver = driver
        self.database_name = credentials['database']


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

    
