import socket    #Charles Frelet, Ricardo Rey de Espana, Isar Palson, Mussadaq Ehsan
import json
import time
import praw
import threading
from datetime import datetime
import os
from dotenv import load_dotenv
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class RedditStreamer:
    def __init__(self, host='127.0.0.1', port=9999):
        self.host = host
        self.port = port
        self.socket = None
        self.client_socket = None
        self.running = False
        
        client_id = os.getenv('REDDIT_CLIENT_ID')
        client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        user_agent = os.getenv('REDDIT_USER_AGENT')


        self.reddit = praw.Reddit(client_id=client_id,client_secret=client_secret,user_agent=user_agent)

        
        # Multiple subreddits for diverse data
        self.subreddits = [
            'python', 'MachineLearning', 'datascience', 'programming', 
            'technology', 'artificial', 'AskReddit', 'news', 'worldnews'
        ]
        
        # Track processed items to avoid duplicates
        self.processed_items = set()
        
    def extract_references(self, text):
        """Extract user references, subreddit references, and URLs"""
        if not text:
            return [], [], []
            
        # User references (/u/username or u/username)
        user_refs = re.findall(r'/?u/([A-Za-z0-9_-]+)', text, re.IGNORECASE)
        
        # Subreddit references (/r/subreddit or r/subreddit)
        subreddit_refs = re.findall(r'/?r/([A-Za-z0-9_-]+)', text, re.IGNORECASE)
        
        # URL references
        url_refs = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\$ \ $ ,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        
        return user_refs, subreddit_refs, url_refs
    
    def process_post(self, post, subreddit_name):
        """Process a Reddit post into structured data"""
        post_id = f"post_{post.id}"
        if post_id in self.processed_items:
            return None
            
        self.processed_items.add(post_id)
        
        # Combine title and selftext for analysis
        full_text = f"{post.title} {post.selftext if post.selftext else ''}"
        
        # Extract references
        user_refs, subreddit_refs, url_refs = self.extract_references(full_text)
        
        data = {
            'id': post.id,
            'type': 'post',
            'subreddit': subreddit_name,
            'title': post.title,
            'text': post.selftext if post.selftext else '',
            'full_text': full_text.strip(),
            'author': str(post.author) if post.author else '[deleted]',
            'score': int(post.score),
            'upvote_ratio': float(post.upvote_ratio) if hasattr(post, 'upvote_ratio') else 0.0,
            'num_comments': int(post.num_comments),
            'created_utc': float(post.created_utc),
            'created_datetime': datetime.fromtimestamp(post.created_utc).isoformat(),
            'url': post.url,
            'permalink': post.permalink,
            'is_self': post.is_self,
            'over_18': post.over_18,
            'spoiler': post.spoiler,
            'locked': post.locked,
            'user_references': user_refs,
            'subreddit_references': subreddit_refs,
            'url_references': url_refs,
            'timestamp_received': datetime.now().isoformat()
        }
        
        return data
    
    def process_comment(self, comment, subreddit_name, post_id):
        """Process a Reddit comment into structured data"""
        comment_id = f"comment_{comment.id}"
        if comment_id in self.processed_items:
            return None
            
        self.processed_items.add(comment_id)
        
        if not comment.body or comment.body in ['[deleted]', '[removed]']:
            return None
        
        # Extract references
        user_refs, subreddit_refs, url_refs = self.extract_references(comment.body)
        
        data = {
            'id': comment.id,
            'type': 'comment',
            'subreddit': subreddit_name,
            'post_id': post_id,
            'text': comment.body,
            'full_text': comment.body,
            'author': str(comment.author) if comment.author else '[deleted]',
            'score': int(comment.score),
            'created_utc': float(comment.created_utc),
            'created_datetime': datetime.fromtimestamp(comment.created_utc).isoformat(),
            'permalink': comment.permalink,
            'is_submitter': comment.is_submitter,
            'user_references': user_refs,
            'subreddit_references': subreddit_refs,
            'url_references': url_refs,
            'timestamp_received': datetime.now().isoformat()
        }
        
        return data
    
    def start_server(self):
        """Start the socket server and begin streaming"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(1)
            
            logger.info(f"Reddit Producer starting on {self.host}:{self.port}")
            logger.info(f"Monitoring subreddits: {', '.join(self.subreddits)}")
            logger.info("Waiting for Spark consumer connection...")
            
            self.client_socket, addr = self.socket.accept()
            logger.info(f"Consumer connected from: {addr}")
            
            self.running = True
            self.stream_reddit_data()
            
        except Exception as e:
            logger.error(f"Server error: {e}")
        finally:
            self.cleanup()
    
    def stream_reddit_data(self):
        """Main streaming loop"""
        logger.info("Starting Reddit data stream...")
        items_sent = 0
        
        while self.running:
            try:
                for subreddit_name in self.subreddits:
                    if not self.running:
                        break
                    
                    logger.info(f"Fetching data from r/{subreddit_name}")
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    # Get recent posts
                    for post in subreddit.new(limit=3):
                        if not self.running:
                            break
                        
                        # Process post
                        post_data = self.process_post(post, subreddit_name)
                        if post_data:
                            self.send_data(post_data)
                            items_sent += 1
                            time.sleep(1)  # Rate limiting
                        
                        # Get some comments for each post
                        try:
                            post.comments.replace_more(limit=0)  # Don't expand "more comments"
                            for comment in post.comments.list()[:2]:  # Max 2 comments per post
                                if not self.running:
                                    break
                                    
                                comment_data = self.process_comment(comment, subreddit_name, post.id)
                                if comment_data:
                                    self.send_data(comment_data)
                                    items_sent += 1
                                    time.sleep(1)  # Rate limiting
                        except Exception as e:
                            logger.warning(f"Error processing comments: {e}")
                    
                    time.sleep(2)  # Brief pause between subreddits
                
                logger.info(f"Completed cycle. Total items sent: {items_sent}")
                logger.info("Waiting 45 seconds before next cycle...")
                time.sleep(45)  # Wait before next full cycle
                
            except Exception as e:
                logger.error(f"Error in streaming loop: {e}")
                time.sleep(10)
    
    def send_data(self, data):
        """Send data through socket"""
        try:
            if self.client_socket and self.running:
                json_data = json.dumps(data, ensure_ascii=False) + '\n'
                self.client_socket.send(json_data.encode('utf-8'))
                logger.info(f"Sent: {data['type']} from r/{data['subreddit']}")
                return True
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            self.running = False
            return False
    
    def cleanup(self):
        """Clean up connections"""
        self.running = False
        if self.client_socket:
            self.client_socket.close()
        if self.socket:
            self.socket.close()
        logger.info("Producer shut down cleanly")

if __name__ == "__main__":
    producer = RedditStreamer()
    try:
        producer.start_server()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        producer.cleanup()
