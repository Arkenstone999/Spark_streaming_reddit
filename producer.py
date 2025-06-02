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

SUBREDDITS = os.getenv('SUBREDDITS', 'dataisbeautiful,python,spark').split(',')

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

def merge_streams(comment_stream, submission_stream):
    itc = iter(comment_stream)
    its = iter(submission_stream)
    while True:
        yield next(itc)
        yield next(its)


def stream_and_send(conn):
    for sub in SUBREDDITS:
        comment_stream = reddit.subreddit(sub).stream.comments(skip_existing=True)
        submission_stream = reddit.subreddit(sub).stream.submissions(skip_existing=True)
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
