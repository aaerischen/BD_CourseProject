import csv
import pickle
import time
import datetime
from os.path import exists
import googleapiclient.errors
from googleapiclient.discovery import build

API_KEY = '<your-key>'
CHANNEL_ID = 'UCdubelOloxR3wzwJG9x8YqQ' # @tvrain
COMMENTS_CSV_PATH = 'comments.csv'
STATE_FILE_PATH = 'state.pkl'
DELAY_SECONDS = 1 # Delay between API requests in seconds
BATCH_SIZE = 100  # Number of comments to retrieve in each batch

def save_comments_to_csv(comments):
    if exists(COMMENTS_CSV_PATH):
        with open(COMMENTS_CSV_PATH, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(comments)
            print(f"{len(comments)} comments added to a CSV file")
    else:
        with open(COMMENTS_CSV_PATH, 'w'):
            print("The CSV comments file is created")

def save_state_info(state):
    with open(STATE_FILE_PATH, 'wb') as file:
        pickle.dump(state, file)

def load_state_info():
    if exists(STATE_FILE_PATH):
        try:
            with open(STATE_FILE_PATH, 'rb') as file:
                state = pickle.load(file)
                return state
        except (IOError, pickle.UnpicklingError) as e:
            print('Error while loading state information:', e)
            return None
    else:
        with open(STATE_FILE_PATH, 'w'):
            print("The STATE file is created")
        return {}

def retrieve_comments(youtube, next_page_token):
    while True:
        try:
            response = youtube.commentThreads().list(
                part='snippet',
                allThreadsRelatedToChannelId=CHANNEL_ID,
                maxResults=BATCH_SIZE,
                pageToken=next_page_token
            ).execute()
            break
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 403:
                now = datetime.datetime.now()
                retry_time = datetime.datetime(now.year, now.month, now.day, 10, 0)
                if now.hour >= 10:
                    retry_time += datetime.timedelta(days=1)
                print(f'API limit exceeded. Retrying at 10:00 (Moscow)...')
                time.sleep((retry_time-now).total_seconds())
            else:
                print('An error occurred:', e)
                continue

    comments = []
    for item in response.get('items', []):
        snippet = item['snippet']
        video_id = snippet['videoId']
        topLevelComment = snippet['topLevelComment']
        comment = topLevelComment['snippet']
        comment_id = topLevelComment['id']
        comment_text = comment['textDisplay']
        author_display_name = comment['authorDisplayName']
        comment_like_count = comment['likeCount']
        comment_published_at = comment['publishedAt']
        comment_updated_at = comment['updatedAt']
        comments.append([
            video_id, comment_id, comment_text, author_display_name,
            comment_like_count, comment_published_at, comment_updated_at
        ])

    return comments, response.get('nextPageToken')

def retrieve_all_comments(youtube):
    state = load_state_info() # Next page token
    if state:
        next_page_token = state.get('next_page_token', '')
    else:
        # Newest next page token
        next_page_token = youtube.commentThreads().list(
            part='snippet',
            allThreadsRelatedToChannelId=CHANNEL_ID,
        ).execute().get('nextPageToken')
    try:
        while next_page_token:
            comments, next_page_token = retrieve_comments(youtube, next_page_token)
            save_comments_to_csv(comments)
            if not next_page_token:
                break
            state = {'next_page_token': next_page_token}
            save_state_info(state)
            time.sleep(DELAY_SECONDS)

    except googleapiclient.errors.HttpError as e:
        print('An error occurred:', e)

# Connect to YouTube Data API
youtube = build('youtube', 'v3', developerKey=API_KEY)

retrieve_all_comments(youtube)
