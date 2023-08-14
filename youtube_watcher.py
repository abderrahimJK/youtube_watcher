import logging
import sys
import requests
import os
import json
from dotenv import load_dotenv

def load_playlist_items_page(google_api, youtube_playlist_id):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
        "key": google_api,
        "playlistId": youtube_playlist_id,
        "part": "contentDetails"
    })
    return json.dumps(response.json(), indent=4)

def main():
    logging.info("Starting youtube watcher")
    load_dotenv()
    payload = load_playlist_items_page(os.getenv("GOOGLE_API"), os.getenv("PLAYLIST_ID"))
    logging.info("Payload: {}".format(payload))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())