from typing import Callable, Dict
from typing import List, Tuple
import os
import io
import json

import pandas as pd
from langchain.schema.document import Document
from langchain.text_splitter import CharacterTextSplitter, TokenTextSplitter
from google.cloud import storage
from google.oauth2 import service_account



class Chunker:

    def __init__(self) -> None:
       
        credentials = service_account.Credentials.from_service_account_file(
                os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
            )  
        self.client = storage.Client(credentials=credentials)
 
        self.service_account = os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
        self.bucket_name = os.environ.get("GCP_BUCKET_NAME")
        self.bucket = self.client.get_bucket(self.bucket_name)
        self._chunked_documents = []

    @property
    def chunk_texts(self) -> List[str]:
        self._assert_documents_chunked()
        return [chunk.page_content for chunk in self._chunked_documents]

    @property
    def chunk_urls(self) -> List[str]:
        self._assert_documents_chunked()
        return list({chunk.metadata.get('source', '') for chunk in self._chunked_documents})

    @property
    def chunks_as_dict(self) -> Dict[str, List[str]]:
        self._assert_documents_chunked()

        result = {}
        for k in self.chunk_urls:
            result.update({k: dict()})
        
        for chunk in self._chunked_documents:
            result.get(chunk.metadata.get('source')).update({"video_id": chunk.metadata.get("video_id"),
                                                             "title": chunk.metadata.get("title"),
                                                             "transcript": chunk.page_content
                                                             })

        return result

    @property
    def chunks_as_list(self) -> Dict[str, List[str]]:
        self._assert_documents_chunked()

        result = []
        
        for chunk in self._chunked_documents:
            result+=[   
                        {
                        "video_id": chunk.metadata.get("video_id"),
                        "video_address": 'https://www.youtube.com/watch?v='+chunk.metadata.get("video_id"),
                        "title": chunk.metadata.get("title"),
                        "transcript": chunk.page_content
                        }
                    ]

        return result

    def _assert_documents_chunked(self):
        if not self._chunked_documents:
            raise ValueError("Documents have not been chunked yet. Call chunk_youtube_transcripts() first.")
    
    @staticmethod
    def _process_youtube_id(id) -> str:
        """
        This method strips the address and file suffix from a retrieved id in the 
        transcript loading process.
        """
        result = id.replace("youtube/transcripts/", "")
        return result.replace(".json", "")
    
    # def _get_transcript_text(self, id: str) -> str:
    #     """
    #     This method gets the transcript text from the GCP storage bucket address.
    #     """

    #     transcript = self.bucket.get_blob("youtube/transcripts/"+id+".txt").download_as_text()
        
    #     return transcript
    
    def _get_video_info(self, id: str) -> Dict[str, str]:

        info = json.loads(self.bucket.get_blob("youtube/transcripts/"+id+".json").download_as_text())
        
        return info

    def _scrape_youtube_transcripts_into_langchain_docs(self, id_list: List[str] = None) -> Tuple[List[Document], List[str]]:
        """
        This method retrieves all YouTube transcripts listed in the provided file list from GCP Storage.
        It then formats them into LangChain Documents. 
        """

        docs = []
        unsuccessful = []

        # grab all trancript file addresses in bucket and format to get ids
        if not id_list:
            id_list = [self._process_youtube_id(blob.name) for blob in self.client.list_blobs(self.bucket_name, prefix="youtube/transcripts/")][1:]

        # grab the transcripts and format into LangChain docs
        for id in id_list:
            try:
                info = self._get_video_info(id)
                url = "https://www.youtube.com/watch?v="+id
                docs.append(Document(page_content=info['transcript'], metadata={"video_id": id,
                                                                                "source": url, 
                                                                                "title": info['title']}))
            except Exception as e:
                print(f"Error loading document with id: {id}")
                print(f"Error: {e}")
                unsuccessful.append(id)

        return docs, unsuccessful

    # def _split_into_chunks(self, documents: List[Document],
    #                        splitter: Callable[[List[Document]], List[Document]]) -> List[Document]:
    #     return splitter.split_documents(documents)
    
    def chunk_youtube_transcripts(self, ids: List[str] = None) -> None:
      
        splitter = TokenTextSplitter(
            chunk_size=512,
            chunk_overlap=64)

        # Start scraping
        documents, failed = self._scrape_youtube_transcripts_into_langchain_docs(ids)

        # Start splitting
        # chunked_docs = self._split_into_chunks(documents, splitter)
        chunked_docs = splitter.split_documents(documents=documents)

        self._chunked_documents.extend(chunked_docs)

