import os
from typing import List

import getpass
# from langchain.embeddings.spacy_embeddings import SpacyEmbeddings
from langchain_community.embeddings.spacy_embeddings import SpacyEmbeddings
from langchain_openai import OpenAIEmbeddings
from langchain_community.embeddings import OllamaEmbeddings


class EmbeddingService:

    def __init__(self, service_provider: str, api_key: str = None) -> None:
        """
        SpaCy embedding service.
        """
        self._select_service(service_provider=service_provider, api_key=api_key)
    
    def _select_service(self, service_provider: str, api_key: str) -> None:
        """
        Select the embedding service to use.
        """

        match service_provider:
            case "spacey":
                self.embedding_service = SpacyEmbeddings(model_name="en_core_web_sm")
                self.dimensions = 300
            case "openai":
                if os.environ["OPENAI_API_KEY"] is None:
                    os.environ["OPENAI_API_KEY"] = api_key or getpass.getpass()
                self.embedding_service = OpenAIEmbeddings(model="text-embedding-3-small", dimensions=1024)
                self.dimensions = 1024
            case "mistral-ollama":
                self.embedding_service = OllamaEmbeddings(model="mistral:7b")
                self.dimensions = 1024
    
    def get_document_embedding(self, chunk_text: str) -> List[float]:
        """
        Get embedding for a single document text string.
        """
 
        return self.embedding_service.embed_query(chunk_text)