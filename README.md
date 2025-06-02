# Spark Streaming Reddit Example

This repository demonstrates streaming Reddit posts and comments over a socket and processing them with Spark Structured Streaming.

## Usage

1. Build the Docker image:
   ```bash
   docker build -t reddit-spark .
   ```
2. Run the container:
   ```bash
   docker run -p 8888:8888 -p 9998:9998 reddit-spark
   ```
3. Open the Jupyter interface and execute `producer.ipynb` to start streaming data from Reddit.
4. In a separate notebook or terminal, run `consumer.ipynb` or `consumer.py` to process the data.

Environment variables `CLIENT_ID` and `SECRET_TOKEN` must be provided for Reddit API access.
