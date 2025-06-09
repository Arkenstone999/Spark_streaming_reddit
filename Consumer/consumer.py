from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml import Pipeline
import time
import os
import shutil
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditAnalytics:
    def __init__(self):
        # Create Spark Session with optimized configuration
        self.spark = SparkSession.builder \
            .appName("RedditRealTimeAnalytics") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Data paths
        self.raw_data_path = "/tmp/reddit_raw_data"
        self.metrics_data_path = "/tmp/reddit_metrics_data"
        self.checkpoint_path = "/tmp/reddit_checkpoint"
        
        # Initialize paths
        self.setup_paths()
        
        # Define schemas
        self.reddit_schema = self.get_reddit_schema()
        self.metrics_schema = self.get_metrics_schema()
        
    def setup_paths(self):
        """Setup and clean data paths"""
        paths = [self.raw_data_path, self.metrics_data_path, self.checkpoint_path]
        for path in paths:
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
        logger.info("Data paths initialized")
    
    def get_reddit_schema(self):
        """Define the schema for Reddit data"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("full_text", StringType(), True),
            StructField("author", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("upvote_ratio", FloatType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("created_utc", DoubleType(), True),
            StructField("created_datetime", StringType(), True),
            StructField("url", StringType(), True),
            StructField("permalink", StringType(), True),
            StructField("post_id", StringType(), True),
            StructField("is_self", BooleanType(), True),
            StructField("is_submitter", BooleanType(), True),
            StructField("over_18", BooleanType(), True),
            StructField("spoiler", BooleanType(), True),
            StructField("locked", BooleanType(), True),
            StructField("user_references", ArrayType(StringType()), True),
            StructField("subreddit_references", ArrayType(StringType()), True),
            StructField("url_references", ArrayType(StringType()), True),
            StructField("timestamp_received", StringType(), True)
        ])
    
    def get_metrics_schema(self):
        """Define the schema for metrics data"""
        return StructType([
            StructField("batch_id", LongType(), True),
            StructField("processing_time", StringType(), True),
            StructField("total_records", LongType(), True),
            StructField("time_range_seconds", LongType(), True),
            StructField("earliest_post", StringType(), True),
            StructField("latest_post", StringType(), True),
            StructField("posts_count", LongType(), True),
            StructField("comments_count", LongType(), True),
            StructField("unique_subreddits", LongType(), True),
            StructField("unique_authors", LongType(), True),
            StructField("avg_score", DoubleType(), True),
            StructField("max_score", LongType(), True),
            StructField("min_score", LongType(), True),
            StructField("total_user_references", LongType(), True),
            StructField("total_subreddit_references", LongType(), True),
            StructField("total_url_references", LongType(), True),
            StructField("top_subreddit", StringType(), True),
            StructField("most_active_author", StringType(), True)
        ])
    
    def calculate_tfidf(self, texts_df):
        """Calculate TF-IDF for the given texts"""
        try:
            # Tokenizer
            tokenizer = Tokenizer(inputCol="full_text", outputCol="words")
            
            # Remove stop words
            stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
            
            # TF
            hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000)
            
            # IDF
            idf = IDF(inputCol="raw_features", outputCol="features")
            
            # Create pipeline
            pipeline = Pipeline(stages=[tokenizer, stop_words_remover, hashing_tf, idf])
            
            # Fit and transform
            model = pipeline.fit(texts_df)
            tfidf_df = model.transform(texts_df)
            
            return tfidf_df.select("id", "filtered_words", "features")
            
        except Exception as e:
            logger.error(f"Error calculating TF-IDF: {e}")
            return texts_df.select("id").withColumn("tfidf_error", lit(str(e)))
    
    def process_batch(self, batch_df, batch_id):
        """Process each batch of streaming data"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"\n{'='*80}")
            logger.info(f"PROCESSING BATCH #{batch_id} at {current_time}")
            logger.info(f"{'='*80}")
            
            batch_count = batch_df.count()
            if batch_count == 0:
                logger.info("Empty batch - skipping processing")
                return
            
            logger.info(f"Processing {batch_count} records")
            
            # 1. SAVE RAW DATA
            logger.info("1. SAVING RAW DATA TO TABLE 'raw' AND FILES")
            batch_df.createOrReplaceGlobalTempView("raw")
            
            # Save raw data to files (partitioned by subreddit)
            raw_batch_path = f"{self.raw_data_path}/batch_{batch_id}"
            batch_df.write.mode("overwrite").partitionBy("subreddit").parquet(raw_batch_path)
            logger.info(f"✓ Raw data saved to {raw_batch_path}")
            
            # 2. REFERENCE ANALYSIS (Users, Subreddits, URLs)
            logger.info("2. ANALYZING REFERENCES")
            
            # User references analysis
            user_refs_df = batch_df.select(
                col("id"),
                col("subreddit"),
                col("timestamp_received"),
                explode(col("user_references")).alias("user_ref")
            ).filter(col("user_ref").isNotNull())
            
            user_ref_counts = user_refs_df.groupBy("user_ref") \
                .agg(count("*").alias("mention_count")) \
                .orderBy(desc("mention_count"))
            
            # Subreddit references analysis
            subreddit_refs_df = batch_df.select(
                col("id"),
                col("subreddit"),
                col("timestamp_received"),
                explode(col("subreddit_references")).alias("subreddit_ref")
            ).filter(col("subreddit_ref").isNotNull())
            
            subreddit_ref_counts = subreddit_refs_df.groupBy("subreddit_ref") \
                .agg(count("*").alias("mention_count")) \
                .orderBy(desc("mention_count"))
            
            # URL references analysis
            url_refs_df = batch_df.select(
                col("id"),
                col("subreddit"),
                col("timestamp_received"),
                explode(col("url_references")).alias("url_ref")
            ).filter(col("url_ref").isNotNull())
            
            url_ref_counts = url_refs_df.groupBy("url_ref") \
                .agg(count("*").alias("mention_count")) \
                .orderBy(desc("mention_count"))
            
            # 3. TF-IDF ANALYSIS
            logger.info("3. CALCULATING TF-IDF FOR TOP WORDS")
            
            texts_for_tfidf = batch_df.filter(
                col("full_text").isNotNull() & 
                (length(col("full_text")) > 10)
            ).select("id", "full_text", "subreddit")
            
            if texts_for_tfidf.count() > 0:
                tfidf_results = self.calculate_tfidf(texts_for_tfidf)
            else:
                tfidf_results = None
            
            # 4. TIME RANGE ANALYSIS
            logger.info("4. ANALYZING TIME RANGES")
            
            time_stats = batch_df.agg(
                min("created_utc").alias("earliest_post"),
                max("created_utc").alias("latest_post"),
                count("*").alias("total_items")
            ).collect()[0]
            
            if time_stats['earliest_post'] and time_stats['latest_post']:
                earliest = datetime.fromtimestamp(time_stats['earliest_post'])
                latest = datetime.fromtimestamp(time_stats['latest_post'])
                time_range = latest - earliest
            else:
                time_range = timedelta(0)
                earliest = latest = None
            
            # 5. SUBREDDIT ANALYSIS
            logger.info("5. ANALYZING SUBREDDIT STATISTICS")
            
            subreddit_stats = batch_df.groupBy("subreddit", "type") \
                .agg(
                    count("*").alias("item_count"),
                    avg("score").alias("avg_score"),
                    sum("num_comments").alias("total_comments"),
                    max("score").alias("max_score"),
                    min("score").alias("min_score")
                ).orderBy(desc("item_count"))
            
            # 6. AUTHOR ANALYSIS
            logger.info("6. ANALYZING AUTHOR ACTIVITY")
            
            author_stats = batch_df.groupBy("author") \
                .agg(
                    count("*").alias("posts_count"),
                    avg("score").alias("avg_score"),
                    sum("num_comments").alias("total_comments")
                ).filter(col("author") != "[deleted]") \
                .orderBy(desc("posts_count"))
            
            # 7. CREATE METRICS SUMMARY WITH PROPER SCHEMA
            logger.info("7. CREATING METRICS SUMMARY")
            
            # Calculate summary statistics
            overall_stats = batch_df.agg(
                avg("score").alias("avg_score"),
                max("score").alias("max_score"),
                min("score").alias("min_score")
            ).collect()[0]
            
            posts_count = batch_df.filter(col("type") == "post").count()
            comments_count = batch_df.filter(col("type") == "comment").count()
            unique_subreddits = batch_df.select("subreddit").distinct().count()
            unique_authors = batch_df.filter(col("author") != "[deleted]").select("author").distinct().count()
            
            # Get top subreddit and author
            top_subreddit_row = subreddit_stats.first()
            top_subreddit = top_subreddit_row["subreddit"] if top_subreddit_row else "unknown"
            
            top_author_row = author_stats.first()
            top_author = top_author_row["author"] if top_author_row else "unknown"
            
            # Create metrics with proper types
            metrics_data = [(
                int(batch_id),                                          # batch_id
                current_time,                                           # processing_time
                int(batch_count),                                       # total_records
                int(time_range.total_seconds()),                        # time_range_seconds
                str(earliest) if earliest else None,                    # earliest_post
                str(latest) if latest else None,                        # latest_post
                int(posts_count),                                       # posts_count
                int(comments_count),                                    # comments_count
                int(unique_subreddits),                                 # unique_subreddits
                int(unique_authors),                                    # unique_authors
                float(overall_stats['avg_score']) if overall_stats['avg_score'] else 0.0,  # avg_score
                int(overall_stats['max_score']) if overall_stats['max_score'] else 0,      # max_score
                int(overall_stats['min_score']) if overall_stats['min_score'] else 0,      # min_score
                int(user_ref_counts.count()),                           # total_user_references
                int(subreddit_ref_counts.count()),                      # total_subreddit_references
                int(url_ref_counts.count()),                            # total_url_references
                str(top_subreddit),                                     # top_subreddit
                str(top_author)                                         # most_active_author
            )]
            
            # Create DataFrame with explicit schema
            metrics_df = self.spark.createDataFrame(metrics_data, self.metrics_schema)
            metrics_df.createOrReplaceGlobalTempView("metrics")
            
            # Save metrics to files
            metrics_batch_path = f"{self.metrics_data_path}/batch_{batch_id}"
            metrics_df.write.mode("overwrite").json(metrics_batch_path)
            logger.info(f"✓ Metrics saved to {metrics_batch_path}")
            
            # 8. SAVE DETAILED ANALYSIS TO SEPARATE FILES
            logger.info("8. SAVING DETAILED ANALYSIS")
            
            # Save reference analysis
            if user_ref_counts.count() > 0:
                user_ref_counts.write.mode("overwrite").json(f"{metrics_batch_path}_user_refs")
            if subreddit_ref_counts.count() > 0:
                subreddit_ref_counts.write.mode("overwrite").json(f"{metrics_batch_path}_subreddit_refs")
            if url_ref_counts.count() > 0:
                url_ref_counts.write.mode("overwrite").json(f"{metrics_batch_path}_url_refs")
            
            # Save subreddit and author stats
            subreddit_stats.write.mode("overwrite").json(f"{metrics_batch_path}_subreddit_stats")
            author_stats.write.mode("overwrite").json(f"{metrics_batch_path}_author_stats")
            
            # 9. DISPLAY RESULTS
            self.display_batch_results(batch_df, subreddit_stats, user_ref_counts, 
                                     subreddit_ref_counts, url_ref_counts, author_stats, metrics_df)
            
            logger.info(f"✓ Batch {batch_id} processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
            import traceback
            traceback.print_exc()
    
    def display_batch_results(self, batch_df, subreddit_stats, user_refs, 
                            subreddit_refs, url_refs, author_stats, metrics_df):
        """Display batch processing results"""
        
        print("\n" + "="*60)
        print("BATCH PROCESSING RESULTS")
        print("="*60)
        
        # Show metrics summary
        print(f"📊 BATCH METRICS SUMMARY:")
        metrics_df.show(truncate=False)
        
        if subreddit_stats.count() > 0:
            print(f"\n📱 TOP SUBREDDITS:")
            subreddit_stats.show(5, truncate=False)
        
        if user_refs.count() > 0:
            print(f"\n👤 TOP USER REFERENCES:")
            user_refs.show(5, truncate=False)
        
        if subreddit_refs.count() > 0:
            print(f"\n🔗 TOP SUBREDDIT REFERENCES:")
            subreddit_refs.show(5, truncate=False)
        
        if url_refs.count() > 0:
            print(f"\n🌐 TOP URL REFERENCES:")
            url_refs.show(5, truncate=False)
        
        if author_stats.count() > 0:
            print(f"\n✍️ TOP AUTHORS:")
            author_stats.show(5, truncate=False)
        
        print(f"\n📈 SAMPLE RAW DATA:")
        batch_df.select("type", "subreddit", "author", "score", "full_text") \
               .show(3, truncate=True)
        
        print("="*60)
    
    def start_streaming(self):
        """Start the streaming process"""
        try:
            logger.info("="*80)
            logger.info("REDDIT REAL-TIME ANALYTICS SYSTEM - SPARK CONSUMER")
            logger.info("="*80)
            logger.info(f"Spark Version: {self.spark.version}")
            logger.info(f"Raw Data Path: {self.raw_data_path}")
            logger.info(f"Metrics Data Path: {self.metrics_data_path}")
            
            # Create streaming DataFrame
            logger.info("Creating streaming connection to Reddit producer...")
            
            df = self.spark \
                .readStream \
                .format("socket") \
                .option("host", "127.0.0.1") \
                .option("port", 9999) \
                .option("includeTimestamp", "true") \
                .load()
            
            # Parse JSON data
            logger.info("Setting up data parsing and transformations...")
            
            parsed_df = df.select(
                from_json(col("value"), self.reddit_schema).alias("data"),
                col("timestamp").alias("spark_timestamp")
            ).select("data.*", "spark_timestamp")
            
            # Add processing timestamp
            processed_df = parsed_df.withColumn("processing_timestamp", current_timestamp())
            
            # Start the streaming query
            logger.info("Starting streaming query...")
            
            query = processed_df.writeStream \
                .foreachBatch(self.process_batch) \
                .trigger(processingTime='30 seconds') \
                .option("checkpointLocation", self.checkpoint_path) \
                .start()
            
            logger.info("="*80)
            logger.info("REDDIT STREAMING ANALYTICS - ACTIVE")
            logger.info("="*80)
            logger.info(f"Stream ID: {query.id}")
            logger.info("Processing batches every 30 seconds")
            logger.info("Listening on 127.0.0.1:9999")
            logger.info("Press Ctrl+C to stop the stream")
            logger.info("="*80)
            
            # Keep the stream running
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            logger.info("Stopping Spark session...")
            self.spark.stop()

if __name__ == "__main__":
    analytics = RedditAnalytics()
    try:
        analytics.start_streaming()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        analytics.spark.stop()
