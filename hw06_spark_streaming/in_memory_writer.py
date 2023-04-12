from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

'''
    Versions of packages
'''
scala_version = "2.12"
spark_version = "3.3.1"
kafka_version = "3.3.1"
mysql_version = "8.0.31"

'''
    Import jars from maven central.
    Follows Format -> groupId:artifactId:version
'''
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    f"org.apache.kafka:kafka-clients:{kafka_version}",
    f"mysql:mysql-connector-java:{mysql_version}"
]

# Spark Imports
spark = (
    SparkSession.builder
    .appName("StructuredWordCount")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)

# Subscribe to wordcounts with offset set to earliest
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "wordCounts")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert topic values, select required columns and filter for latest messages.
stream_df = (
    stream_df
    .withColumn("word", stream_df["key"].cast("string").alias("word"))
    .drop("key")
    .withColumn("count", stream_df["value"].cast("string").alias("count"))
    .drop("value")
    .select("word", "count", "timestamp")
    .where("timestamp >= '2023-03-12 19:00:00'")
    .limit(20)
)


# Convert to json
stream_df_json = (
    stream_df
    .withColumn("words_json", to_json(struct("word", "count", "timestamp")))
    .select("words_json")
)


# In memory sink
stream_query = (
    stream_df_json
    .writeStream
    .format("memory")
    .queryName("word_counts")
    .outputMode("append")
    .trigger(once=True)
    .start()
)

stream_query.awaitTermination()

spark.sql("select * from word_counts").show()


