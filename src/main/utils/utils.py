from typing import List, Iterator, Any, Dict
from uuid import uuid4
import time

from tools.embedding import EmbeddingService

def batch_method(data: List[Any], batch_size: int) -> Iterator[List[Any]]:
    for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

def remove_filler_words(text: str) -> str:
    """
    Remove filler words from a text string.
    """

    filler_words = ["um", "ah", "uh"]

    for word in filler_words:
        text = text.replace(word, "")

    # remove extra whitespace
    return text.strip()

def prepare_new_nodes(data: List[Dict[str,str]], embedding_service: EmbeddingService, playlist_id: str = "") -> List[Dict]:
    """
    format chunked data to be uploaded into neo4j graph.
    embedding must abide by rate limits: 60 requests / minute
    """

    new_nodes = data.copy()

    for chunk in new_nodes:
        

        start = time.time()

        # make request
        chunk.update({  "child_index": str(uuid4()),
                        "playlist_id": playlist_id,
                        "embedding": embedding_service.get_document_embedding(chunk['transcript'])})

            
    return new_nodes