import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Spark Session Setup
spark = SparkSession.builder \
    .appName("RedditStreamingConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Session created: {spark.version}")

# Connection Configuration
HOST = "127.0.0.1"
PORT = 9998
METRICS_CHECKPOINT_PATH = "/tmp/spark-checkpoints/reddit_metrics"

print(f"Configuration:")
print(f"HOST: {HOST}")
print(f"PORT: {PORT}")
print(f"CHECKPOINT: {METRICS_CHECKPOINT_PATH}")

# Data Schema Definition
reddit_schema = StructType([
    StructField("type", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("message", StringType(), True)
])

print("Schema defined for Reddit data")

# Create Streaming DataFrame
print("Creating streaming connection...")

raw_stream = spark.readStream \
    .format("socket") \
    .option("host", HOST) \
    .option("port", PORT) \
    .option("includeTimestamp", "true") \
    .load()

print("Raw stream created")

# Parse JSON data and apply schema
parsed_stream = raw_stream.select(
    from_json(col("value"), reddit_schema).alias("data"),
    col("timestamp").alias("processing_time")
).select("data.*", "processing_time")

# Filter out heartbeat messages for analytics
reddit_data = parsed_stream.filter(col("type") != "heartbeat")

print("Parsed stream created with filtering")

# Global variables for monitoring
query = None
batch_count = 0

def process_batch(batch_df, batch_id):
    """Process each batch of streaming data"""
    global batch_count
    batch_count += 1
    
    print("=" * 70)
    print(f"PROCESSING BATCH #{batch_id}")
    print(f"Batch received at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get batch size
    record_count = batch_df.count()
    print(f"Records in batch: {record_count}")
    
    if record_count == 0:
        print("Empty batch - skipping processing")
        return
    
    # Cache the batch for multiple operations
    batch_df.cache()
    
    # Type distribution
    print("\nTYPE DISTRIBUTION:")
    type_counts = batch_df.groupBy("type").count().orderBy(desc("count")).collect()
    for row in type_counts:
        print(f"{row['type']}: {row['count']} records")
    
    # Top subreddits
    print("\nTOP SUBREDDITS IN BATCH:")
    subreddit_counts = batch_df.filter(col("subreddit").isNotNull()) \
                               .groupBy("subreddit").count() \
                               .orderBy(desc("count")) \
                               .limit(10).collect()
    
    for i, row in enumerate(subreddit_counts, 1):
        print(f"{i}. r/{row['subreddit']}: {row['count']} posts")
    
    # Text analytics
    print("\nTEXT ANALYTICS:")
    text_stats = batch_df.filter(col("text").isNotNull() & (col("text") != "")) \
                         .select(
                             length(col("text")).alias("text_length"),
                             size(split(col("text"), " ")).alias("word_count")
                         ).agg(
                             avg("text_length").alias("avg_length"),
                             avg("word_count").alias("avg_words"),
                             max("text_length").alias("max_length"),
                             min("text_length").alias("min_length")
                         ).collect()[0]
    
    print(f"Average text length: {text_stats['avg_length']:.1f} chars")
    print(f"Average word count: {text_stats['avg_words']:.1f} words")
    print(f"Longest text: {text_stats['max_length']} chars")
    print(f"Shortest text: {text_stats['min_length']} chars")
    
    # Author analytics
    print("\nAUTHOR ANALYTICS:")
    author_stats = batch_df.filter(col("author").isNotNull() & (col("author") != "[deleted]"))
    unique_authors = author_stats.select("author").distinct().count()
    print(f"Unique authors: {unique_authors}")
    
    top_authors = author_stats.groupBy("author").count() \
                              .orderBy(desc("count")) \
                              .limit(5).collect()
    
    if top_authors:
        print("Most active authors:")
        for author in top_authors:
            print(f"  {author['author']} ({author['count']} posts)")
    
    # Score analytics for submissions/comments with scores
    score_data = batch_df.filter(col("score").isNotNull() & (col("score") > 0))
    if score_data.count() > 0:
        print("\nSCORE ANALYTICS:")
        score_stats = score_data.agg(
            avg("score").alias("avg_score"),
            max("score").alias("max_score"),
            min("score").alias("min_score")
        ).collect()[0]
        
        print(f"Average score: {score_stats['avg_score']:.1f}")
        print(f"Highest score: {score_stats['max_score']}")
        print(f"Lowest score: {score_stats['min_score']}")
    
    # Time analytics
    print("\nTIME ANALYTICS:")
    current_time = time.time()
    time_stats = batch_df.filter(col("created_utc").isNotNull()) \
                         .select(((current_time - col("created_utc")) / 60).alias("age_minutes")) \
                         .agg(
                             avg("age_minutes").alias("avg_age"),
                             min("age_minutes").alias("newest"),
                             max("age_minutes").alias("oldest")
                         ).collect()[0]
    
    print(f"Average post age: {time_stats['avg_age']:.1f} minutes")
    print(f"Newest post: {time_stats['newest']:.1f} minutes ago")
    print(f"Oldest post: {time_stats['oldest']:.1f} minutes ago")
    
    # Sample records
    print("\nSAMPLE RECORDS:")
    sample_records = batch_df.select("type", "subreddit", "author", "text") \
                             .limit(3).collect()
    
    for i, record in enumerate(sample_records, 1):
        text_preview = record['text'][:100] + "..." if record['text'] and len(record['text']) > 100 else record['text']
        print(f"{i}. [{record['type']}] r/{record['subreddit']} by {record['author']}")
        print(f"   Text: {text_preview}")
    
    # Keyword detection
    print("\nKEYWORD DETECTION:")
    keywords = ['python', 'machine learning', 'AI', 'data', 'programming', 'technology', 'science']
    
    for keyword in keywords:
        keyword_count = batch_df.filter(col("text").contains(keyword)).count()
        if keyword_count > 0:
            print(f"'{keyword}': {keyword_count} mentions")
    
    # Unpersist the cached DataFrame
    batch_df.unpersist()
    
    print(f"\nBatch #{batch_id} processing completed")
    print("=" * 70)

# Helper Functions
def get_query_status():
    """Get current query status"""
    if query and query.isActive:
        progress = query.lastProgress
        print(f"Query ID: {query.id}")
        print(f"Query Status: Active")
        print(f"Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"Input Rows: {progress.get('inputRowsPerSecond', 0)}")
        print(f"Processing Rate: {progress.get('processedRowsPerSecond', 0)}")
    else:
        print("Query is not active")

def monitor_streaming_query(duration_minutes=5):
    """Monitor the streaming query for specified duration"""
    print(f"Monitoring streaming query for {duration_minutes} minutes...")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    while time.time() < end_time and query and query.isActive:
        progress = query.lastProgress
        
        if progress:
            print(f"[{time.strftime('%H:%M:%S')}] Batch: {progress.get('batchId', 'N/A')}, "
                  f"Input Rate: {progress.get('inputRowsPerSecond', 0):.1f} rows/sec, "
                  f"Processing Rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
        
        time.sleep(10)
    
    print("Monitoring completed")

def stop_streaming():
    """Stop the streaming query"""
    global query
    if query and query.isActive:
        query.stop()
        print("Streaming query stopped")
    else:
        print("No active query to stop")

# Start the streaming query
print("Starting streaming query...")

query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", METRICS_CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming query started successfully")

# Query monitoring
print(f"Query ID: {query.id}")
print(f"Query Status: {query.status}")
print(f"Is Active: {query.isActive}")

print("\n" + "=" * 70)
print("REDDIT STREAMING CONSUMER STARTED")
print("=" * 70)

print(f"Waiting for data from producer at {HOST}:{PORT}")
print(f"Processing batches every 10 seconds")
print(f"Checkpoints saved to: {METRICS_CHECKPOINT_PATH}")

print(f"\nAVAILABLE COMMANDS:")
print(f"monitor_streaming_query(minutes) - Monitor for X minutes")
print(f"get_query_status() - Check current status")
print(f"stop_streaming() - Stop the stream")

print(f"\nSTREAM IS RUNNING - Start your producer now!")
print(f"Use: start_producer() in your producer notebook")

# Auto-monitor for 2 minutes to show initial activity
print(f"\nAuto-monitoring for 2 minutes...")
monitor_streaming_query(duration_minutes=2)