# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import col
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("TweetSentimentAnalysis") \
#     .getOrCreate()

# # Read from socket
# df = spark.readStream \
#     .format("socket") \
#     .option("host", "127.0.0.1") \
#     .option("port", 5000) \
#     .load()

# # Split CSV line into columns
# from pyspark.sql.functions import split

# columns = ["textID", "text", "sentiment", "Time of Tweet", "Age of User", "Country", "Population -2020", "Land Area (Km2)", "Density (P/Km2)"]
# split_df = df.select(split(df["value"], ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").alias("cols")) \
# 	     .select([col("cols").getItem(i).alias(columns[i]) for i in range(len(columns))]) \
#              .toDF(*columns)

# # Set up VADER
# analyzer = SentimentIntensityAnalyzer()

# def analyze_sentiment(text):
#     scores = analyzer.polarity_scores(text)
#     compound = scores["compound"]
#     if compound >= 0.05:
#         return "positive"
#     elif compound <= -0.05:
#         return "negative"
#     else:
#         return "neutral"

# sentiment_udf = udf(analyze_sentiment, StringType())

# # Processing each batch
# def process_batch(df, epoch_id):
#     if not df.isEmpty():
#         df_with_pred = df.withColumn("predicted_sentiment", sentiment_udf(df["text"]))
#         df_with_pred.show(truncate=False)

# # Start streaming with custom processing
# query = split_df.writeStream \
#     .foreachBatch(process_batch) \
#     .start()

# query.awaitTermination()



# updated code from here


# ********************************************************
















# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, split, col
# from pyspark.sql.types import StringType
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# # Add at the beginning of api_server.py
# import os
# if not os.path.exists('output'):
#     os.makedirs('output')
# if not os.path.exists('checkpoint'):
#     os.makedirs('checkpoint')

# spark = SparkSession.builder.appName("TweetSentimentAnalysis").getOrCreate()

# df = spark.readStream \
#     .format("socket") \
#     .option("host", "127.0.0.1") \
#     .option("port", 5000) \
#     .load()

# columns = ["textID", "text", "sentiment", "Time of Tweet", "Age of User", "Country", "Population -2020", "Land Area (Km2)", "Density (P/Km2)"]

# split_df = df.select(split(df["value"], ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").alias("cols")) \
# 	     .select([col("cols").getItem(i).alias(columns[i]) for i in range(len(columns))])

# analyzer = SentimentIntensityAnalyzer()

# def analyze_sentiment(text):
#     scores = analyzer.polarity_scores(text)
#     compound = scores["compound"]
#     if compound >= 0.05:
#         return "positive"
#     elif compound <= -0.05:
#         return "negative"
#     else:
#         return "neutral"

# sentiment_udf = udf(analyze_sentiment, StringType())

# df_with_pred = split_df.withColumn("predicted_sentiment", sentiment_udf(col("text")))

# query = df_with_pred.writeStream \
#     .format("json") \
#     .option("path", "output/") \
#     .option("checkpointLocation", "checkpoint/") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()




from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, col, regexp_replace, from_json
from pyspark.sql.types import StringType, StructType, StructField
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TweetSentimentAnalysis")

# Ensure output directories exist
os.makedirs('output', exist_ok=True)
os.makedirs('checkpoint', exist_ok=True)

# Define schema for proper JSON parsing
schema = StructType([
    StructField("textID", StringType(), True),
    StructField("text", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("Time of Tweet", StringType(), True),
    StructField("Age of User", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Population -2020", StringType(), True),
    StructField("Land Area (Km²)", StringType(), True),
    StructField("Density (P/Km²)", StringType(), True)
])

# Create Spark session with better configuration
spark = SparkSession.builder \
    .appName("TweetSentimentAnalysis") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from socket with better timeout handling
df = spark.readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 5000) \
    .option("includeTimestamp", True) \
    .load()

# Parse JSON data properly
parsed_df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Clean data
for column in parsed_df.columns:
    parsed_df = parsed_df.withColumn(column, 
        regexp_replace(col(column), '^\\s*"|"\\s*$', ''))

# Sentiment analysis
analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    try:
        if not text or str(text).strip() == "":
            return "neutral"
        scores = analyzer.polarity_scores(str(text))
        compound = scores["compound"]
        if compound >= 0.05:
            return "positive"
        elif compound <= -0.05:
            return "negative"
        return "neutral"
    except Exception as e:
        logger.error(f"Sentiment analysis error: {str(e)}")
        return "neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

# Add predicted sentiment
result_df = parsed_df.withColumn("predicted_sentiment", sentiment_udf(col("text")))

# Define streaming query with better configuration
# query = result_df.writeStream \
#     .format("json") \
#     .outputMode("append") \
#     .option("path", "output") \
#     .option("checkpointLocation", "checkpoint") \
#     .trigger(processingTime="10 seconds") \
#     .option("maxFilesPerTrigger", 1) \
#     .start()
# In the query configuration, change the trigger to:
query = result_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "output") \
    .option("checkpointLocation", "checkpoint") \
    .trigger(processingTime="5 seconds") \
    .option("maxFilesPerTrigger", 1) \
    .start()


logger.info("Streaming query started")

# Add shutdown hook
import atexit
def shutdown_hook():
    logger.info("Shutting down streaming query...")
    query.stop()
    spark.stop()
    logger.info("Streaming stopped gracefully")

atexit.register(shutdown_hook)

query.awaitTermination()