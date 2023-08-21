import logging
from pprint import pformat
import sys
import requests
import os
import json
from dotenv import load_dotenv

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
    payload = load_playlist_items_page(google_api, youtube_playlist_id, page_token=None) 

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

def main():
    logging.info("Starting youtube watcher")
    load_dotenv()
    google_api = os.getenv("GOOGLE_API")
    playlist_id = os.getenv("PLAYLIST_ID")

    payload = fetch_playlist_items(google_api, playlist_id)
    for videoItem in payload:
        video_id = videoItem["contentDetails"]["videoId"]
        for video in fetch_videos(google_api, video_id):
            logging.info(pformat(summarize_video(video)))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())