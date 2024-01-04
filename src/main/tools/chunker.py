from typing import List, Tuple, Callable, Dict
import os
import io
import json
from uuid import uuid4

import pandas as pd
from langchain.schema.document import Document
from langchain.text_splitter import CharacterTextSplitter, TokenTextSplitter, RecursiveCharacterTextSplitter
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
    def chunks_as_list(self) -> Dict[str, List[str]]:
        self._assert_documents_chunked()

        result = []
        
        for chunk in self._chunked_documents:
            result+=[{
                    "url": "https://www.youtube.com/watch?v="+chunk.metadata.get("video_id"),
                    "video_id": chunk.metadata.get("video_id"),
                    "parent_index": chunk.metadata.get("parent_index"),
                    "parent_transcript": chunk.metadata.get("parent_transcript"),
                    "title": chunk.metadata.get("title"),
                    "publish_date": chunk.metadata.get("publish_date"),
                    "transcript": chunk.page_content
                    }]

        return result

    def _assert_documents_chunked(self):
        if not self._chunked_documents:
            raise ValueError("Documents have not been chunked yet. Call chunk_youtube_transcripts() first.")
    
    @staticmethod
    def _process_youtube_id(id, playlist_title: str) -> str:
        """
        This method strips the address and file suffix from a retrieved id in the 
        transcript loading process.
        """

        if playlist_title != "":
            playlist_title+="/"
        
        result = id.replace("youtube/transcripts/"+playlist_title, "")
        return result.replace(".json", "")
    
    def _get_youtube_video_info(self, id: str, playlist_title: str = "") -> Dict[str, str]:

        if playlist_title != "":
            playlist_title+="/"

        info = json.loads(self.bucket.get_blob("youtube/transcripts/"+playlist_title+id+".json").download_as_text())
        
        return info
    
    def _scrape_youtube_transcripts_into_langchain_docs(self, id_list: List[str] = None, playlist_title: str = "") -> Tuple[List[Document], List[str]]:
        """
        This method retrieves all YouTube transcripts listed in the provided file list from GCP Storage.
        It then formats them into LangChain Documents. 
        """

        docs = []
        unsuccessful = []

        # grab all trancript file addresses in bucket and format to get ids
        if not id_list:
            id_list = [self._process_youtube_id(blob.name, playlist_title) for blob in self.client.list_blobs(self.bucket_name, prefix="youtube/transcripts/"+playlist_title)]

        # grab the transcripts and format into LangChain docs
        for id in id_list:
            try:
                info = self._get_youtube_video_info(id, playlist_title=playlist_title)
                 
                docs.append(Document(page_content=info['transcript'], metadata={"video_id": id,
                                                                                "source": "https://www.youtube.com/watch?v="+id, 
                                                                                "publish_date": info['publish_date'],
                                                                                "title": info['title'],
                                                                                }))
            except Exception as e:
                print(f"Error loading document with id: {id}")
                print(f"Error: {e}")
                unsuccessful.append(id)

        return docs, unsuccessful
    
    def _clean_chunked_documents(self, chunked_docs: List[Document], cleaning_functions: List[Callable[[str], str]]) -> \
            List[Document]:
        for i, doc in enumerate(chunked_docs):
            for func in cleaning_functions:
                doc.page_content = func(doc.page_content)
            chunked_docs[i] = doc
        return chunked_docs
    
    def _create_child_docs(self, docs: List[Document], splitter: Callable[[List[Document]], List[Document]]) -> List[Document]:
        """
        Create child documents given a list of parent documents.
        """

        return_list = []

        for i, doc in enumerate(docs):
            # set the parent index before splitting
            doc.metadata['parent_index'] = str(uuid4())
            temp_split_docs = splitter.split_documents(documents=[doc])

            for child in temp_split_docs:
                child.metadata['parent_transcript'] = doc.page_content
                return_list+=[child]

        return return_list

    def chunk_youtube_transcripts(self, 
                                  ids: List[str] = None,
                                  playlist_title: str = "",
                                  cleaning_functions: List[Callable[[str], str]] = None) -> None:
        """
        Perform the chunking process for transcripts in the provided id list and playlist.
        """

        # primary splitter
        primary_splitter = TokenTextSplitter(
            chunk_size=512,
            chunk_overlap=64)
        
        # set to ~= average length of question
        secondary_splitter = RecursiveCharacterTextSplitter(
            chunk_size=140,
            chunk_overlap=35
        ) 
        # Start scraping
        documents, failed = self._scrape_youtube_transcripts_into_langchain_docs(ids, playlist_title=playlist_title)

        # Create the Parent documents
        chunked_docs = primary_splitter.split_documents(documents=documents)

        # Create the child documents
        chunked_docs = self._create_child_docs(chunked_docs, secondary_splitter)

        # clean chunks
        chunked_docs = self._clean_chunked_documents(chunked_docs, cleaning_functions)

        self._chunked_documents.extend(chunked_docs)

