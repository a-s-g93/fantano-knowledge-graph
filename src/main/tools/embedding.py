from typing import List

from langchain.embeddings.spacy_embeddings import SpacyEmbeddings



class EmbeddingService:

    def __init__(self) -> None:
        """
        SpaCy embedding service.
        """
        self.embedding_service = SpacyEmbeddings()
    
    def get_document_embedding(self, chunk_text: str) -> List[float]:
        """
        Get embedding for a single document text string.
        """
 
        return self.embedding_service.embed_query(chunk_text)