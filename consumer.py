import json
import re
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

HOST = 'host.docker.internal'
PORT = 9998

spark = SparkSession.builder.appName('RedditConsumer').getOrCreate()

schema = StructType([
    StructField('type', StringType()),
    StructField('subreddit', StringType()),
    StructField('id', StringType()),
    StructField('text', StringType()),
    StructField('created_utc', DoubleType()),
    StructField('author', StringType())
])

raw_lines = (
    spark.readStream
    .format('socket')
    .option('host', HOST)
    .option('port', PORT)
    .load()
)

json_df = raw_lines.select(F.from_json(F.col('value'), schema).alias('data')).select('data.*')

# Write incoming data to memory table 'raw' and to files
def start_storage_queries(df):
    q1 = (
        df.writeStream
        .outputMode('append')
        .format('memory')
        .queryName('raw')
        .start()
    )
    q2 = (
        df.writeStream
        .outputMode('append')
        .format('parquet')
        .option('path', 'data/raw')
        .option('checkpointLocation', 'chk/raw')
        .start()
    )
    return [q1, q2]

# Reference counting
def start_reference_query(df):
    user_refs = F.expr("regexp_extract_all(text, '/u/[^\\s]+')")
    sub_refs = F.expr("regexp_extract_all(text, '/r/[^\\s]+')")
    url_refs = F.expr("regexp_extract_all(text, 'https?://[^\\s]+')")

    refs_df = df.select(
        F.col('created_utc').cast('timestamp').alias('created_ts'),
        F.size(user_refs).alias('user_ref_count'),
        F.size(sub_refs).alias('sub_ref_count'),
        F.size(url_refs).alias('url_ref_count')
    )

    windowed = (
        refs_df.withWatermark('created_ts', '1 minute')
        .groupBy(F.window('created_ts', '60 seconds', '5 seconds'))
        .sum('user_ref_count', 'sub_ref_count', 'url_ref_count')
    )

    return (
        windowed.writeStream
        .outputMode('update')
        .format('console')
        .option('truncate', False)
        .start()
    )

# TF-IDF computation
def compute_tfidf(full_df):
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

    tokenizer = Tokenizer(inputCol='text', outputCol='words')
    words = tokenizer.transform(full_df)
    remover = StopWordsRemover(inputCol='words', outputCol='filtered')
    filtered = remover.transform(words)
    hashingTF = HashingTF(inputCol='filtered', outputCol='rawFeatures', numFeatures=10000)
    featurized = hashingTF.transform(filtered)
    idf = IDF(inputCol='rawFeatures', outputCol='features')
    idf_model = idf.fit(featurized)
    tfidf = idf_model.transform(featurized)
    zipped = tfidf.select(F.explode(F.arrays_zip('filtered', 'features')).alias('z'))
    scores = zipped.select(F.col('z.filtered').alias('word'), F.col('z.features').alias('score'))
    top_words = scores.groupBy('word').agg(F.max('score').alias('score')).orderBy(F.desc('score')).limit(10)
    print('Top words by TF-IDF:')
    top_words.show(truncate=False)

# Sentiment UDF
@F.udf('double')
def sentiment_udf(text):
    return float(TextBlob(text).sentiment.polarity) if text else 0.0

# Batch processing
def process_batch(df, epoch_id):
    df.persist()
    if spark.catalog.tableExists('raw'):
        full_df = spark.table('raw').unionByName(df)
    else:
        full_df = df
    full_df.createOrReplaceTempView('raw')

    # Save batch to files
    df.write.mode('append').parquet('data/raw')

    # Time range
    bounds = full_df.agg(F.min('created_utc').alias('min_ts'), F.max('created_utc').alias('max_ts')).collect()[0]
    print(f"Data time range: {bounds['min_ts']} - {bounds['max_ts']}")

    # Sentiment
    sentiments = df.withColumn('sentiment', sentiment_udf('text'))
    avg_sent = sentiments.agg(F.avg('sentiment')).collect()[0][0]
    print(f'Average sentiment (batch): {avg_sent}')

    # Top authors in this batch
    top_authors = df.groupBy('author').count().orderBy(F.desc('count')).limit(5)
    print('Top authors this batch:')
    top_authors.show(truncate=False)

    # TF-IDF on full corpus
    compute_tfidf(full_df)
    df.unpersist()

# Start queries
storage_queries = start_storage_queries(json_df)
ref_query = start_reference_query(json_df)
process_query = json_df.writeStream.foreachBatch(process_batch).start()

for q in storage_queries + [ref_query, process_query]:
    q.awaitTermination()
