from typing import Callable, Dict
from typing import List, Tuple
import os
import sys
import io
import requests
import json

import pandas as pd
# from langchain.schema.document import Document
# from langchain.text_splitter import CharacterTextSplitter, TokenTextSplitter
from google.cloud import storage
from google.oauth2 import service_account
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter


class Scraper:
    """
    This class provides methods for scraping transcripts from YouTube and loading new data into storage buckets.
    """

    def __init__(self, channel_id: str = None, playlist_id: str = None) -> None:
       
        credentials = service_account.Credentials.from_service_account_file(
                os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
            )  
        self.client = storage.Client(credentials=credentials)
 
        self.service_account = os.environ.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
        self.bucket_name = os.environ.get("GCP_BUCKET_NAME")

        if self.client.bucket(self.bucket_name).exists:
            self.bucket = self.client.get_bucket(self.bucket_name)
        else:
            raise ValueError("Bucket does not exist. Please create bucket via GCP console and try again.")
            

        # track failed transcript creations
        # This can happen with unaired live videos
        self._unsuccessful_list = None

        self._channel_id = self._get_channel_id() if not channel_id else channel_id
        self._playlist_id = self._get_playlist_id() if not playlist_id else playlist_id

        self._scraped_video_info = None

    @property
    def channel_id(self) -> str:
        return self._channel_id
    
    @channel_id.setter
    def channel_id(self, channel_id: str) -> None:
        self._channel_id = channel_id
    
    @property
    def playlist_id(self) -> str:
        return self._playlist_id
    
    @playlist_id.setter
    def playlist_id(self, playlist_id: str) -> None:
        self._playlist_id = playlist_id
    
    def add_new_youtube_urls(self) -> None:
        """
        This method checks for new youtube videos and updates the csv file in GCP Storage.
        """

        pass

    def _get_youtube_video_info(self, video_info_file_name: str) -> pd.DataFrame:

        videos_temp = self.bucket.get_blob("youtube/"+video_info_file_name+".csv")
        videos_temp = videos_temp.download_as_string()
        videos = pd.read_csv(io.BytesIO(videos_temp))[['id', 'title', 'publish_date']]
        return videos
    
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
    
    def _upload_transcript(self, transcript: str, video_id: str, title: str, publish_date: str, folder: str = "") -> None:
        """
        This method uploads a transcript to GCP Storage.
        Uploading a file will automatically overwrite any existing file with the same id in storage.
        """

        if folder != "":
            folder += "/"

        file_loc = "youtube/transcripts/"+folder
        to_upload = {"video_id": video_id,
                     "title": title,
                     "publish_date": publish_date,
                     "transcript": transcript}
        self.bucket.blob(file_loc+video_id+".json").upload_from_string(json.dumps(to_upload), content_type='application/json')

    # TODO: Multi-thread this process
    def create_and_upload_transcripts(self, transcripts_folder: str = "", video_info_file_name: str = "video_info") -> List[str]:
        """
        This method:
        1. gets the list of all neo4j video urls from GCP Storage.
        2. gets a transcript of each video. If unsuccessful, is added to list to be returned.
        3. uploads the transcript to GCP Storage.

        returns:
            None
        """

        unsuccessful = []
        fail_count = 0

        videos = self._get_youtube_video_info(video_info_file_name=video_info_file_name)

        total = len(videos)

        for idx, info in videos.iterrows():
            id = info['id']
            try:
                transcript = self._create_transcript(video_id=id)
                self._upload_transcript(transcript=transcript, 
                                        video_id=id, title=info['title'], 
                                        publish_date=info['publish_date'],
                                        folder=transcripts_folder)

            except Exception as e:
                # print("Failed: "+id)
                fail_count+=1
                unsuccessful+=[{"id": id,
                                "title": info['title'],
                                "publish_date": info['publish_date']}]
            
            if idx % 2 == 0:
                print("Progress : ", round((idx+1) / total * 100, 2), "% | ", "failed: ", fail_count, end="\r")
         

        # print("failed count: ", len(unsuccessful))
        print("Progress : ", round((idx+1) / total * 100, 2), "% | ", "failed: ", fail_count, end="\r")


        self._unsuccessful_list = unsuccessful 

    def update_unsuccessful_transcripts(self, file_name: str = "failed_video_info") -> None:
        """
        This method takes a list of unsuccessful YouTube ids and creates a new csv file
        in GCP Storage for later retrieval. 
        """

        file_loc = "youtube/"

        # create failed df
        failed_df = pd.DataFrame.from_dict(self._unsuccessful_list)

        # check if failed df in GCP storage
        if storage.Blob(bucket=self.bucket, name=file_loc+file_name+".csv").exists(self.client):
            # pull failed df from storage
            videos_temp = self.bucket.get_blob(file_loc+file_name+".csv")
            videos_temp = videos_temp.download_as_string()

            previous_failed_df =  pd.read_csv(io.BytesIO(videos_temp))[['id', 'title', 'publish_date']]

            # append new failed df
            new_failed_df = pd.concat(previous_failed_df, failed_df)
            
        else:
            # create first failed df
            new_failed_df = failed_df

        # upload new df to storage
        self.bucket.blob(file_loc+"failed_video_info.csv").upload_from_string(new_failed_df.to_csv(), 'text/csv')

    @staticmethod
    def _get_channel_id() -> str:
        """
        This method retrieves the channel id for the "The Needle Drop" YouTube channel.
        """

        address = f"https://www.googleapis.com/youtube/v3/search?q=the+needle+drop&key={os.environ.get('YOUTUBE_API_KEY')}&part=snippet"
        req = requests.get(address)
        data = req.json()

        return data['items'][0]['snippet']['channelId']
    
    def _get_playlist_id(self) -> str:
        """
        This method retrieves the uploads id for the channel of interest.
        """

        address = f"https://www.googleapis.com/youtube/v3/channels?id={self.channel_id}&key={os.environ.get('YOUTUBE_API_KEY')}&part=contentDetails"
        try:
            req = requests.get(address)
            data = req.json()
        except ValueError as e:
            print(e, "Provide accurate channel id.")

        return data['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    def scrape_video_info(self, next_page_token: str = None, total_results: int = -1, videos: List[str] = []) -> List[str]:
            
        address = f"https://www.googleapis.com/youtube/v3/playlistItems?playlistId={self.playlist_id}&key={os.environ.get('YOUTUBE_API_KEY')}&part=snippet&maxResults=50"
        
        if not next_page_token:
            vid_req = requests.get(address)

        else:
            vid_req = requests.get(address+f'&pageToken={next_page_token}')
            
        vids = vid_req.json()

        if total_results == -1:
            total_results = vids['pageInfo']['totalResults']
            print('total results set: ', total_results)

        videos += [{"id": x['snippet']['resourceId']['videoId'], 
                    "title": x['snippet']['title'],
                    "publish_date": x['snippet']['publishedAt'][:10]} for x in vids['items']]


        print("total results: ", total_results, end='\r')
        print("ids retrieved: ", len(videos), "\r")

        if "nextPageToken" not in vids.keys():
            print("complete")
            self._scraped_video_info = videos
            return videos
        
        self.scrape_video_info(next_page_token=vids['nextPageToken'], total_results=total_results, videos=videos)
    
    def upload_scraped_video_info(self, file_name: str = "video_info") -> None:
        """
        This method uploads the scraped video ids to a new csv in the designated GCP Storage bucket.
        """

        file_loc = "youtube/"

        data = pd.DataFrame.from_dict(self._scraped_video_info)

        self.bucket.blob(file_loc+file_name+".csv").upload_from_string(data.to_csv(), 'text/csv')

        
