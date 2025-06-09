import os
import socket
import json
import time
import praw
from threading import Thread
import queue

# Configuration
HOST = os.getenv('PRODUCER_HOST', '127.0.0.1')
PORT = int(os.getenv('PRODUCER_PORT', '9998'))
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('SECRET_TOKEN')
USER_AGENT = os.getenv('USER_AGENT', 'MyRedditApp/0.0.1')

# Multiple active subreddits for more data
SUBREDDITS = 'python+MachineLearning+datascience+programming+technology+artificial+AskReddit'

print(f"Configuration:")
print(f"HOST: {HOST}, PORT: {PORT}")
print(f"SUBREDDITS: {SUBREDDITS}")
print(f"CLIENT_ID configured: {'Yes' if CLIENT_ID else 'No'}")

# Initialize Reddit
reddit = praw.Reddit(
    client_id=CLIENT_ID, 
    client_secret=CLIENT_SECRET, 
    user_agent=USER_AGENT
)

def stream_comments(data_queue):
    """Stream comments from all subreddits"""
    try:
        subreddit = reddit.subreddit(SUBREDDITS)
        print(f"Starting comment stream for multiple subreddits")
        
        for comment in subreddit.stream.comments(skip_existing=True, pause_after=5):
            if comment is None:
                continue
                
            try:
                payload = {
                    'type': 'comment',
                    'subreddit': str(comment.subreddit),
                    'id': comment.id,
                    'text': comment.body[:500],  # Limit text length
                    'created_utc': comment.created_utc,
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'score': comment.score if hasattr(comment, 'score') else 0
                }
                
                if not data_queue.full():
                    data_queue.put(payload, timeout=1)
                    print(f"Queued comment from r/{payload['subreddit']}")
                    
            except Exception as e:
                print(f"Error processing comment: {e}")
                continue
                
    except Exception as e:
        print(f"Comment stream error: {e}")

def stream_submissions(data_queue):
    """Stream submissions from all subreddits"""
    try:
        subreddit = reddit.subreddit(SUBREDDITS)
        print(f"Starting submission stream for multiple subreddits")
        
        for submission in subreddit.stream.submissions(skip_existing=True, pause_after=5):
            if submission is None:
                continue
                
            try:
                text = submission.title
                if submission.selftext:
                    text += ' ' + submission.selftext[:500]  # Limit text length
                    
                payload = {
                    'type': 'submission', 
                    'subreddit': str(submission.subreddit),
                    'id': submission.id,
                    'text': text,
                    'created_utc': submission.created_utc,
                    'author': str(submission.author) if submission.author else '[deleted]',
                    'score': submission.score if hasattr(submission, 'score') else 0,
                    'num_comments': submission.num_comments if hasattr(submission, 'num_comments') else 0
                }
                
                if not data_queue.full():
                    data_queue.put(payload, timeout=1)
                    print(f"Queued submission from r/{payload['subreddit']}")
                    
            except Exception as e:
                print(f"Error processing submission: {e}")
                continue
                
    except Exception as e:
        print(f"Submission stream error: {e}")

def send_heartbeat(data_queue):
    """Send periodic heartbeat to keep connection alive"""
    while True:
        try:
            heartbeat = {
                'type': 'heartbeat',
                'timestamp': time.time(),
                'message': 'connection_alive'
            }
            if not data_queue.full():
                data_queue.put(heartbeat, timeout=1)
            time.sleep(30)  # Heartbeat every 30 seconds
        except Exception as e:
            print(f"Heartbeat error: {e}")

def start_producer():
    """Main producer function"""
    # Create queue with reasonable size limit
    data_queue = queue.Queue(maxsize=100)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((HOST, PORT))
        server_sock.listen(1)
        print(f'Producer: Listening on {HOST}:{PORT}')
        
        conn, addr = server_sock.accept()
        print(f'Producer: Connected to {addr}')
        
        with conn:
            # Start all streams in separate threads
            Thread(target=stream_comments, args=(data_queue,), daemon=True).start()
            Thread(target=stream_submissions, args=(data_queue,), daemon=True).start()
            Thread(target=send_heartbeat, args=(data_queue,), daemon=True).start()
            
            print("Started streaming threads for comments, submissions, and heartbeat")
            
            try:
                message_count = 0
                while True: 
                    try:
                        # Get data with timeout to prevent blocking
                        payload = data_queue.get(timeout=5)
                        message_count += 1
                        
                        # Send data
                        json_str = json.dumps(payload) + '\n'
                        conn.sendall(json_str.encode('utf-8'))
                        
                        if payload['type'] != 'heartbeat':
                            print(f"Sent #{message_count}: {payload['type']} from r/{payload.get('subreddit', 'unknown')}")
                        
                        # Small delay to prevent overwhelming
                        time.sleep(0.1)
                        
                    except queue.Empty:
                        # No data available, continue waiting
                        print("No data in queue, waiting...")
                        continue
                        
            except (ConnectionResetError, BrokenPipeError):
                print("Client disconnected")
            except KeyboardInterrupt:
                print("Shutting down...")
            except Exception as e:
                print(f"Producer error: {e}")

if __name__ == "__main__":
    start_producer()