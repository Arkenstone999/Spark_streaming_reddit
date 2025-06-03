import json
import re
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

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

def start_storage_queries(df):

    # Write to memory table 'raw'
    q1 = (
        df.writeStream
        .outputMode('append')
        .format('memory')
        .queryName('raw')
        .start()
    )

    # Write to parquet file 'raw' in folder 'data' and checkpoint in 'chk'
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
    # Extract user, subreddit, and URL references
    # in a more clean manner.
    user_refs = F.regexp_extract_all("text", r"/u/[^\s]+")
    sub_refs = F.regexp_extract_all("text", r"/r/[^\s]+")
    url_refs = F.regexp_extract_all("text", r"https?://[^\s]+")

    # All of this is clean code, well done.
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
    # This requires a full explanation and overview of the TF-IDF process,
    # It also requires the full dataframe to be available when you only need to retrieve the text column.
    # This is not ideal for production, but it works for the sake of this example.

    # Tokenize the text column and remove stop words
    tokenizer = Tokenizer(inputCol='text', outputCol='words')
    words = tokenizer.transform(full_df)
    remover = StopWordsRemover(inputCol='words', outputCol='filtered')
    filtered = remover.transform(words)

    # Apply HashingTF and create an IDF (Inverse Document Frequency). WARNING: Hashing TF is a lossy transformation,
    # meaning that it can produce collisions in the feature space.
    # What does that mean? It means that two different words can end up with the same hash value
    # and thus the same feature vector. It means that the TF-IDF score for those words will be the same,
    # even if they are different words. This would be even more problematic if the stop words were not removed.
    hashingTF = HashingTF(inputCol='filtered', outputCol='rawFeatures', numFeatures=10000)
    featurized = hashingTF.transform(filtered)
    idf = IDF(inputCol='rawFeatures', outputCol='features')
    idf_model = idf.fit(featurized)
    tfidf = idf_model.transform(featurized)

    # Extract top 10 words by TF-IDF score
    # Ideally this should be done per post, not the full corpus that
    # extends across 3 different subreddits. Makes little sense to compute the TF-IDF
    # for that corpus with no differentiation between the subreddits.
    # His requirements would not be met in a production environment, at least not in this way, keep that in mind.
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
def process_batch(df):
    df.persist()
    if spark.catalog.tableExists('raw'):
        full_df = spark.table('raw').unionByName(df)
    else:
        full_df = df
    full_df.createOrReplaceTempView('raw')

    # Save batch to files
    df.write.mode('append').parquet('data/raw')

    # Time range
    created_utc_min, created_utc_max = F.min(df['created_utc']).alias('min_ts'), F.max(df['created_utc']).alias('max_ts')
    bounds = full_df.agg(created_utc_min, created_utc_max).collect()[0]
    print(f"Data time range: {bounds['min_ts']} - {bounds['max_ts']}")

    # Sentiment
    sentiments = df.withColumn('sentiment', sentiment_udf('text'))
    avg_sent = sentiments.agg(F.avg('sentiment')).collect()[0][0]
    print(f'Average sentiment (batch): {avg_sent}')

    # Top authors in this batch
    top_authors = df.groupBy('author').count().orderBy(F.desc('count')).limit(5)
    print('Top authors this batch:')
    top_authors.show(truncate=False)

    # TF-IDF on full corpus (all texts in all comments) is a heavy operation so it should be done less frequently in production.
    # His requirements would not be met in a production environment, they would be orchestrated so as not to
    # have to retrieve all the texts every time.
    # However, for the sake of his example, we will compute it every time.
    compute_tfidf(full_df)
    df.unpersist()

# Start queries
storage_queries = start_storage_queries(json_df)
ref_query = start_reference_query(json_df)
process_query = json_df.writeStream.foreachBatch(process_batch).start()

for q in storage_queries + [ref_query, process_query]:
    q.awaitTermination()
