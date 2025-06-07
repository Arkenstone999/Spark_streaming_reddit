#### Utility Functions
#### Import as you please with
#### from utils import * as utils
#### or
#### from utils import {function_name}

import json
import requests
import praw


def request_json(url, params=None):
    """
    Make a GET request to the specified URL and return the JSON response.
    
    Args:
        url (str): The URL to send the request to.
        params (dict, optional): Dictionary of query parameters to include in the request.
        
    Returns:
        dict: The JSON response from the server.
    """
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()


def load_json(file_path):
    """
    Load a JSON file from the specified path.
    
    Args:
        file_path (str): The path to the JSON file.
        
    Returns:
        dict: The contents of the JSON file.
    """
    with open(file_path, 'r') as file:
        return json.load(file)
    
def initialize_reddit_client(client_id, client_secret, user_agent):
    """
    Initialize a Reddit client using PRAW.
    
    Args:
        client_id (str): The client ID for the Reddit API.
        client_secret (str): The client secret for the Reddit API.
        user_agent (str): A unique user agent string for the application.
        
    Returns:
        praw.Reddit: An instance of the Reddit client.
    """
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )

def stream_posts_json(subreddit, sock):
    reddit_prefix = "https://www.reddit.com"
    for post in subreddit.stream.submissions():  # Changed from comments to submissions
        try:
            my_object = {
                "id": post.id,  # Unique identifier for the post
                "title": post.title,  # Added post title
                "selftext": post.selftext,  # Post body/content
                "author": str(post.author),  # Post author
                "url": post.url,  # Post URL
                "permalink": f"{reddit_prefix}{post.permalink}",  # Post permalink
                "created_utc": post.created_utc,  # Creation timestamp
                "ups": post.ups,  # Upvotes
                "score": post.score,  # Score (net upvotes)
                "num_comments": post.num_comments  # Number of comments
            }
            json_encoded = json.dumps(my_object)
            print(json_encoded)
        except praw.exceptions.PRAWException as ex:
            pass

def stream_comments(subreddit, reddit, sock):
    for comment in subreddit.stream.comments():
        try:
            post = comment.submission
            parent_id = str(comment.parent())
            try:
                prev_comment = reddit.comment(parent_id)
                prev_body = prev_comment.body
            except praw.exceptions.PRAWException:
                prev_body = None  # Handle cases where parent comment is inaccessible

            comment_object = {
                "type": "comment",
                "comment": comment.body,
                "prev_comment": prev_body,
                "post_title": post.title,
                "post_id": post.id,
                "author": str(comment.author),
                "link_url": comment.link_url,
                "link_permalink": comment.link_permalink,
                "created_utc": comment.created_utc,
                "ups": comment.ups,
                "likes": comment.likes
            }
            json_encoded = json.dumps(comment_object)
            print(json.dumps(comment_object))

            # Send through socket
            c, addr = sock.accept()
            c.send(json_encoded.encode('utf-8'))
            c.close()
        except praw.exceptions.PRAWException as ex:
            pass
    


