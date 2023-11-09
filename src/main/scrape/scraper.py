from typing import Optional, Callable, Dict, Any
from typing import List, Tuple
# from itertools import chain
import pandas as pd
from langchain.document_loaders import UnstructuredURLLoader
from langchain.schema.document import Document
from langchain.text_splitter import CharacterTextSplitter, TokenTextSplitter
from google.cloud import storage
from google.oauth2 import service_account
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
# import json
import os
import io
# import time


class WebContentChunker:

    def __init__(self, client: storage.Client = None) -> None:
        if not client:
            credentials = service_account.Credentials.from_service_account_file(
                    os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
                )  
            self.client = storage.Client(credentials=credentials)
        else:
            self.client = client

        self.service_account = os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
        self.bucket_name = "agent-neo-neo4j-cs-team-201901-docs"
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
            result.update({k: []})
        
        for chunk in self._chunked_documents:
            result.get(chunk.metadata.get('source')).append(chunk.page_content)

        return result

    def _assert_documents_chunked(self):
        if not self._chunked_documents:
            raise ValueError("Documents have not been chunked yet. Call chunk_documents() first.")

    def _scrape_sites_into_langchain_docs(self, resources: List[str]) -> List[Document]:
        try:
            loader = UnstructuredURLLoader(urls=resources)
            return loader.load()
        except Exception as e:
            print(f"Error loading documents from URLs: {e}")
            return []
    
    @staticmethod
    def _process_youtube_id(id) -> str:
        """
        This method strips the address and file suffix from a retrieved id in the 
        transcript loading process.
        """
        result = id.replace("youtube/transcripts/", "")
        return result.replace(".txt", "")
    
    def _get_transcript_text(self, id: str) -> str:
        """
        This method gets the transcript text from the GCP storage bucket address.
        """

        transcript = self.bucket.get_blob("youtube/transcripts/"+id+".txt").download_as_text()
        
        return transcript

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
                transcript = self._get_transcript_text(id)
                url = "https://www.youtube.com/watch?v="+id
                docs.append(Document(page_content=transcript, metadata={"source": url}))
            except Exception as e:
                print(f"Error loading document with id: {id}")
                print(f"Error: {e}")
                unsuccessful.append(id)

        return docs, unsuccessful

    def _split_into_chunks(self, documents: List[Document],
                           splitter: Callable[[List[Document]], List[Document]]) -> List[Document]:
        return splitter.split_documents(documents)

    def _clean_chunked_documents(self, chunked_docs: List[Document], cleaning_functions: List[Callable[[str], str]]) -> \
            List[Document]:
        for i, doc in enumerate(chunked_docs):
            for func in cleaning_functions:
                doc.page_content = func(doc.page_content)
            chunked_docs[i] = doc
        return chunked_docs

    def chunk_documents(self, urls: List[str],
                        splitter: Callable[[List[Document]], List[Document]] = None,
                        cleaning_functions: List[Callable[[str], str]] = None) -> None:

        if splitter is None:
            splitter = CharacterTextSplitter(
                separator="\n",
                chunk_size=1024,
                chunk_overlap=128)

        # Start scraping
        documents = self._scrape_sites_into_langchain_docs(urls)

        # Start splitting
        chunked_docs = self._split_into_chunks(documents, splitter)

        if cleaning_functions:
            chunked_docs = self._clean_chunked_documents(chunked_docs, cleaning_functions)

        self._chunked_documents.extend(chunked_docs)
    
    def chunk_youtube_transcripts(self, ids: List[str] = None,
                        splitter: Callable[[List[Document]], List[Document]] = None,
                        cleaning_functions: List[Callable[[str], str]] = None) -> None:

        if splitter is None:
            splitter = TokenTextSplitter(
                chunk_size=512,
                chunk_overlap=64)

        # Start scraping
        documents, failed = self._scrape_youtube_transcripts_into_langchain_docs(ids)

        # Start splitting
        chunked_docs = self._split_into_chunks(documents, splitter)

        if cleaning_functions:
            chunked_docs = self._clean_chunked_documents(chunked_docs, cleaning_functions)

        self._chunked_documents.extend(chunked_docs)
        
    def __str__(self) -> str:
        return "\n".join([f"Document: {doc}" for doc in self._chunked_documents])

class GCPStorageLoader:
    """
    This class provides methods for loading new data into the Agent Neo storage buckets.
    """

    def __init__(self, client: storage.Client = None) -> None:
        if not client:
            credentials = service_account.Credentials.from_service_account_file(
                    os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
                )  
            self.client = storage.Client(credentials=credentials)
        else:
            self.client = client

        self.service_account = os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
        self.bucket_name = "agent-neo-neo4j-cs-team-201901-docs"
        self.bucket = self.client.get_bucket(self.bucket_name)

        # track failed transcript creations
        # This can happen with unaired live videos
        self._unsuccessful_list = None

    def add_new_youtube_urls(self) -> None:
        """
        This method checks for new youtube videos and updates the csv file in GCP Storage.
        """

        pass

    def _get_youtube_video_ids(self) -> List[str]:
        """
        This method gets the YouTube video ids csv file from GCP Storage and 
        returns a list of the video urls it contains.
        """
        # bucket = self.client.get_bucket(self.bucket_name)

        videos_temp = self.bucket.get_blob("youtube/neo4j_video_list.csv")
        videos_temp = videos_temp.download_as_string()
        videos_list = pd.read_csv(io.BytesIO(videos_temp))['YouTube_Address'].to_list()

        return videos_list
    
    @staticmethod
    def _create_transcript(video_id: str) -> str:
        """
        This method retrieves the video transcript and formats it.
        Returns a string representation of the video transcript.
        """

        raw_transcript = YouTubeTranscriptApi.get_transcript(video_id)
        # instantiate the text formatter
        formatter = TextFormatter()

        # format the video into a string without timestamps, etc...
        transcript_formatted = formatter.format_transcript(raw_transcript)

        # replace newlines with a space 

        return transcript_formatted.replace("\n", " ")
    
    def _upload_transcript(self, transcript: str, video_id: str) -> None:
        """
        This method uploads a transcript to GCP Storage.
        Uploading a file will automatically overwrite any existing file with the same id in storage.
        """

        file_loc = "youtube/transcripts/"
        self.bucket.blob(file_loc+video_id+".txt").upload_from_string(transcript, 'text/plain')

    def create_and_upload_neo4j_transcripts(self) -> List[str]:
        """
        This method:
        1. gets the list of all neo4j video urls from GCP Storage.
        2. gets a transcript of each video. If unsuccessful, is added to list to be returned.
        3. uploads the transcript to GCP Storage.

        returns:
            unsuccessful: list of video ids that were unable to be transcribed.
        """

        unsuccessful = []

        ids = self._get_youtube_video_ids()
        for id in ids[:10]:
            try:
                transcript = self._create_transcript(video_id=id)
                self._upload_transcript(transcript=transcript, video_id=id)
                print("Video "+id+" Uploaded to GCP Storage.")
            except Exception as e:
                print("Failed: "+id)
                # print(e)
                unsuccessful+=[id]

        return unsuccessful  

    def update_unsuccessful_transcripts(self, unsuccessful_list: List[str]) -> None:
        """
        This method takes a list of unsuccessful YouTube ids and creates a new csv file
        in GCP Storage for later retrieval. 
        """

        file_loc = "youtube/"
        failed_df = pd.Series({"YouTube_Address": unsuccessful_list})
        self.bucket.blob(file_loc+"neo4j_failed_video_list.csv").upload_from_string(failed_df.to_csv(), 'text/csv')