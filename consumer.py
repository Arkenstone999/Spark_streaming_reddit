import json
import re
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
import time
import os

HOST = 'host.docker.internal'
PORT = 9998

# FIXED: Corrected "KryoSerializer" spelling and improved configuration
spark = SparkSession.builder \
    .appName('RedditConsumer') \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.maxResultSize", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema definition
schema = StructType([
    StructField('type', StringType(), True),
    StructField('subreddit', StringType(), True),
    StructField('id', StringType(), True),
    StructField('text', StringType(), True),
    StructField('created_utc', DoubleType(), True),
    StructField('author', StringType(), True)
])

def create_base_stream():
    """Create and return the base streaming DataFrame with enhanced error handling"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"Attempting to connect to {HOST}:{PORT} (attempt {retry_count + 1})")
            
            raw_lines = (
                spark.readStream
                .format('socket')
                .option('host', HOST)
                .option('port', PORT)
                .option('includeTimestamp', 'true')
                .load()
            )
            
            json_df = (
                raw_lines
                .select(F.from_json(F.col('value'), schema).alias('data'))
                .select('data.*')
                .filter(
                    F.col('text').isNotNull() & 
                    (F.col('text') != '') & 
                    (F.length(F.col('text')) > 10)
                )
                .withColumn('created_ts', F.col('created_utc').cast('timestamp'))
                .withColumn('text_length', F.length(F.col('text')))
            )
            
            print("✓ Base stream created successfully")
            return json_df
            
        except Exception as e:
            retry_count += 1
            print(f"✗ Connection attempt {retry_count} failed: {e}")
            
            if retry_count >= max_retries:
                print("Max retries reached. Please ensure:")
                print("1. Producer is running on the correct port")
                print("2. Docker networking is properly configured")
                print("3. No firewall blocking the connection")
                raise e
            
            time.sleep(5)  # Wait before retry

def start_simple_monitoring(df):
    """Start a simple monitoring query to verify stream is working"""
    try:
        monitoring_query = (
            df.select('subreddit', 'author', 'text_length', 'created_ts')
            .writeStream
            .outputMode('append')
            .format('console')
            .option('truncate', True)
            .option('numRows', 3)
            .trigger(processingTime='15 seconds')
            .start()
        )
        
        print("✓ Simple monitoring query started")
        return monitoring_query
        
    except Exception as e:
        print(f"✗ Error starting monitoring query: {e}")
        return None

def start_subreddit_stats(df):
    """Start subreddit statistics with watermarking"""
    try:
        stats_df = (
            df.withWatermark('created_ts', '2 minutes')
            .groupBy(
                F.window('created_ts', '1 minute', '30 seconds'),
                F.col('subreddit')
            )
            .agg(
                F.count('*').alias('post_count'),
                F.countDistinct('author').alias('unique_authors'),
                F.avg('text_length').alias('avg_length'),
                F.max('text_length').alias('max_length')
            )
            .filter(F.col('post_count') > 0)
            .select(
                F.col('window.start').alias('window_start'),
                F.col('window.end').alias('window_end'),
                '*'
            )
        )
        
        stats_query = (
            stats_df.writeStream
            .outputMode('update')
            .format('console')
            .option('truncate', False)
            .option('numRows', 10)
            .trigger(processingTime='30 seconds')
            .start()
        )
        
        print("✓ Subreddit statistics query started")
        return stats_query
        
    except Exception as e:
        print(f"✗ Error starting stats query: {e}")
        return None

def start_reference_analysis(df):
    """Simplified reference analysis"""
    try:
        refs_df = df.select(
            F.col('subreddit'),
            F.col('created_ts'),
            F.regexp_extract_all(F.col('text'), r'/u/\w+').alias('user_mentions'),
            F.regexp_extract_all(F.col('text'), r'/r/\w+').alias('sub_mentions'),
            F.regexp_extract_all(F.col('text'), r'https?://[^\s]+').alias('urls')
        ).select(
            F.col('subreddit'),
            F.col('created_ts'),
            F.size(F.col('user_mentions')).alias('user_ref_count'),
            F.size(F.col('sub_mentions')).alias('sub_ref_count'),
            F.size(F.col('urls')).alias('url_count')
        ).filter(
            (F.col('user_ref_count') > 0) | 
            (F.col('sub_ref_count') > 0) | 
            (F.col('url_count') > 0)
        )
        
        ref_query = (
            refs_df.writeStream
            .outputMode('append')
            .format('console')
            .option('truncate', False)
            .option('numRows', 5)
            .trigger(processingTime='30 seconds')
            .start()
        )
        
        print("✓ Reference analysis query started")
        return ref_query
        
    except Exception as e:
        print(f"✗ Error starting reference analysis: {e}")
        return None

def cleanup_checkpoints():
    """Clean up checkpoint directories"""
    import shutil
    try:
        checkpoint_dir = "/tmp/checkpoint"
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
            print("✓ Cleaned up checkpoint directories")
    except Exception as e:
        print(f"Warning: Could not clean checkpoints: {e}")

def kill_port_process():
    """Kill any process using our target port"""
    try:
        import subprocess
        result = subprocess.run(
            ['lsof', '-ti', f':{PORT}'], 
            capture_output=True, 
            text=True,
            timeout=10
        )
        
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                subprocess.run(['kill', '-9', pid], timeout=5)
                print(f"✓ Killed process {pid} using port {PORT}")
            time.sleep(2)
    except Exception as e:
        print(f"Note: Could not kill port processes: {e}")

# Main execution with enhanced error handling
if __name__ == "__main__":
    queries = []
    
    try:
        print("=" * 50)
        print("🚀 REDDIT STREAM PROCESSOR STARTING")
        print("=" * 50)
        
        # Cleanup previous runs
        cleanup_checkpoints()
        kill_port_process()
        
        # Test Spark context
        print("Testing Spark configuration...")
        test_df = spark.range(1).select(F.lit("test").alias("value"))
        test_count = test_df.count()
        print(f"✓ Spark context working (test count: {test_count})")
        
        # Create base stream
        print("\nCreating base stream...")
        json_df = create_base_stream()
        
        # Start queries progressively
        print("\n📊 Starting monitoring queries...")
        
        # Simple monitoring first
        monitor_query = start_simple_monitoring(json_df)
        if monitor_query:
            queries.append(monitor_query)
            print("Waiting 30 seconds to verify data flow...")
            time.sleep(30)
        
        # Add statistics if monitoring works
        stats_query = start_subreddit_stats(json_df)
        if stats_query:
            queries.append(stats_query)
            time.sleep(10)
        
        # Add reference analysis
        ref_query = start_reference_analysis(json_df)
        if ref_query:
            queries.append(ref_query)
        
        active_queries = [q for q in queries if q and q.isActive]
        print(f"\n✅ Successfully started {len(active_queries)} queries")
        
        # Monitor loop
        print("\n📈 Monitoring queries (Press Ctrl+C to stop)...")
        check_count = 0
        
        while True:
            time.sleep(30)  # Check every 30 seconds
            check_count += 1
            
            active = []
            failed = []
            
            for i, query in enumerate(queries):
                if not query:
                    continue
                    
                if query.isActive:
                    active.append(i)
                    # Show progress less frequently
                    if check_count % 4 == 0:  # Every 2 minutes
                        progress = query.lastProgress
                        if progress:
                            input_rate = progress.get('inputRowsPerSecond', 0)
                            processing_rate = progress.get('processingRowsPerSecond', 0)
                            print(f"Query {i}: Input: {input_rate:.1f}/sec, Processing: {processing_rate:.1f}/sec")
                else:
                    failed.append(i)
                    if query.exception():
                        print(f"❌ Query {i} failed: {query.exception()}")
            
            if check_count % 4 == 0:
                print(f"📊 Status: {len(active)} active, {len(failed)} failed")
            
            if not active:
                print("No active queries remaining")
                break
                
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested by user")
    except Exception as e:
        print(f"❌ Main execution error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🧹 Cleaning up...")
        
        # Stop all queries
        for i, query in enumerate(queries):
            if query and query.isActive:
                try:
                    print(f"Stopping query {i}...")
                    query.stop()
                except Exception as e:
                    print(f"Error stopping query {i}: {e}")
        
        # Wait for shutdown
        for query in queries:
            if query:
                try:
                    query.awaitTermination(timeout=15)
                except:
                    pass
        
        # Stop Spark
        try:
            spark.stop()
            print("✅ Spark session stopped")
        except Exception as e:
            print(f"Warning: Error stopping Spark: {e}")
        
        print("🏁 Cleanup complete")
