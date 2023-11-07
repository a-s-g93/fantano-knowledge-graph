from neo4j.exceptions import ConstraintError
# from google.oauth2 import service_account
# from google.cloud import aiplatform
# import streamlit as st
import drivers
from typing import List, Optional
# from graphdatascience import GraphDataScience
from credentials import credentials
# from vertexai.preview.language_models import TextEmbeddingModel

from langchain.chat_models import ChatVertexAI, AzureChatOpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationSummaryBufferMemory
from langchain.prompts.prompt import PromptTemplate

import time

class Communicator:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    This class contains methods necessary to interact with the Neo4j database
    and manage conversations with the chosen LLM.
    """

    def __init__(self) -> None:

        self.driver = drivers.init_driver(credentials['uri'], username=credentials['username'], password=credentials['password'])
        self.database_name = credentials['database']
        # self.project = st.secrets['gcp_project']
        # self.region = st.secrets['gcp_region']

        # init the aiplatform 
        # aiplatform.init(project=self.project, location=self.region, credentials=google_credentials)

        # instantiate Google text embedding model
        # self.text_embedding_model = TextEmbeddingModel.from_pretrained(TEXT_EMBEDDING_MODEL)

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

    def neo4j_vector_index_search():
        """
        This method runs vector similarity search on the document embeddings against the question embedding.
        """

        return self.gds.run_cypher("""
                                    CALL db.index.vector.queryNodes('document-embeddings', toInteger($k), $questionEmbedding)
                                    YIELD node AS vDocs, score
                                    return vDocs.url as url, vDocs.text as text, vDocs.index as index
                                    """, {'questionEmbedding': embeddings[0], 'k': st.session_state['num_documents_for_context']}
                                    )

    def create_prompt(self, question: str):
        """
        This function creates a prompt to be sent to the LLM.
        """

        context_timer_start = time.perf_counter()
        context = self.create_context(question)

        # use context docs in the prompt
    

        print("Context creation total time: "+str(round(time.perf_counter()-context_timer_start, 4))+" seconds.")

        prompt_template = """
                Follow these steps exactly:
                1. Read this question as an experienced graph data scientist at Neo4j: {question} 
                2. Read and summarize the following context documents, ignoring any that do not relate to the user question: {context}
                3. Use this context and your knowledge to answer the user question.
                4. Return your answer with sources.
                            """
        
        prompt = PromptTemplate(input_variables=["question", "context"], template=prompt_template)
    
        return [
            prompt.format(question=question, context=context[['url', 'text']].to_dict('records')),
            list(context['index'])
        ]
        
    def create_conversation(self, llm_type:str):
        """
        This function intializes a conversation with the llm.
        The resulting conversation can be prompted successively and will
        remember previous interactions.
        """
        create_conversation_timer_start = time.perf_counter()
        print("llm type: ", llm_type)
        llm = self._init_llm(llm_type, st.session_state['temperature'])

        st.session_state['llm_memory'] = ConversationSummaryBufferMemory(llm=llm, max_token_limit=1000)

        res = ConversationChain(
            llm=llm,
            memory=st.session_state['llm_memory']
            ) 
        print("Create conversation time: "+str(round(time.perf_counter()-create_conversation_timer_start, 4))+" seconds.")

        return res