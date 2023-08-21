import logging
from pprint import pformat
from config import config
import sys
import requests
import os
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

#fetch the playlist items
def load_playlist_items_page(google_api, youtube_playlist_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
        "key": google_api,
        "playlistId": youtube_playlist_id,
        "part": "contentDetails",
        "pageToken": page_token,
    })
    
    return json.loads(response.text)

#fetch videos from the playlist
def load_videos_page(google_api, videoId, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", params={
        "key": google_api,
        "id": videoId,
        "part": "snippet, statistics",
        "pageToken": page_token,
    })
    
    return json.loads(response.text)

#fetch items from all playlists pages
def fetch_playlist_items(google_api, youtube_playlist_id, page_token=None):
    payload = load_playlist_items_page(google_api, youtube_playlist_id, page_token) 

    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_playlist_items(google_api, youtube_playlist_id, page_token=next_page_token)

#fetch video details
def fetch_videos(google_api, video_id, page_token=None):
    payload = load_videos_page(google_api, video_id, page_token)

    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_videos(google_api, video_id, page_token=next_page_token)

def summarize_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likeCount", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }

def on_delivery(err, record):
    pass

def main():
    logging.info("Starting youtube watcher")
    schema_registry_client= SchemaRegistryClient(config["schema_registry"])
    google_api = config["google_api_key"]
    playlist_id = config["youtube_playlist_id"]
    youtube_videos_value_schema = schema_registry_client.get_latest_version("youtube_videos-value")
    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_value_schema.schema.schema_str,
        ),
    }
    producer = SerializingProducer(kafka_config)

    payload = fetch_playlist_items(google_api, playlist_id)
    for videoItem in payload:
        video_id = videoItem["contentDetails"]["videoId"]
        for video in fetch_videos(google_api, video_id):
            logging.info(pformat(summarize_video(video)))
            
            producer.produce(
                topic = 'youtube_videos',
                key =   video_id,
                value = {
                    "TITLE": video["snippet"]["title"],
                    "VIEWS": int(video["statistics"].get("viewCount", 0)),
                    "LIKES": int(video["statistics"].get("likeCount", 0)),
                    "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                },
                on_delivery = on_delivery,
            )
    producer.flush()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())