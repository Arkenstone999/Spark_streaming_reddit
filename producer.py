import os
import socket
import json
import time
import praw

HOST = os.getenv('PRODUCER_HOST', '127.0.0.1')
PORT = int(os.getenv('PRODUCER_PORT', '9998'))

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('SECRET_TOKEN')
USER_AGENT = os.getenv('USER_AGENT', 'MyRedditApp/0.0.1')

# If you have an environment variable you retrieve from a secure space and you still set the default, 
# it doesn't need to be an environment variable in a secure space.
SUBREDDITS = ['dataisbeautiful', 'python', 'spark']

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

def merge_streams(comment_stream, submission_stream):
    comment_stream_item = iter(comment_stream)
    submission_stream_item = iter(submission_stream)
    while True:
        yield next(comment_stream_item)
        yield next(submission_stream_item)


def stream_and_send(conn):
    for sub in SUBREDDITS:
        comment_stream = reddit.subreddit(sub).stream.comments(skip_existing=True)
        submission_stream = reddit.subreddit(sub).stream.submissions(skip_existing=True)
         # This will have a side effect when scaled: hardware catches up with the stream and for loop ends
         # This round-robin approach is not ideal for scaling, but it works for a single producer
        for item in merge_streams(comment_stream, submission_stream):
            payload = {
                'type': 'comment' if hasattr(item, 'body') else 'submission',
                'subreddit': sub,
                'id': item.id,
                'text': item.body if hasattr(item, 'body') else (item.title + '\n\n' + (item.selftext or '')),
                'created_utc': item.created_utc,
                'author': str(item.author)
            }
            conn.sendall((json.dumps(payload) + '\n').encode('utf-8'))

             # This solves the issue of the consumer catching up with the producer too quickly, which can lead to high CPU usage and potential socket errors.
             # However, this can be problematic in a production environment where you want to handle backpressure more gracefully.
             # Check out backpressure here: https://medium.com/@beeindian04/back-pressure-in-data-pipeline-bdc25c6c1d79
            time.sleep(0.1)


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((HOST, PORT))
        server_sock.listen(1)
        print(f"Producer: Listening on {HOST}:{PORT} …")
        conn, addr = server_sock.accept()
        print(f"Producer: Connection accepted from {addr}")
        with conn:
            stream_and_send(conn)


if __name__ == '__main__':
    main()
