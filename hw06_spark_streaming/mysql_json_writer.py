from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_json, struct

'''
    Versions of packages
'''
scala_version = '2.12'
spark_version = '3.3.1'
kafka_version = '3.3.1'
mysql_version = '8.0.31'

'''
    Import jars from maven central.
    Follows Format -> groupId:artifactId:version
'''
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    f'org.apache.kafka:kafka-clients:{kafka_version}',
    f'mysql:mysql-connector-java:{mysql_version}'
]

# Build Session
spark = (
    SparkSession.builder
    .appName("StructuredWordCount")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)


# User defined function to write streams to MySql in foreachBatch
def my_sql_sink(df: DataFrame, batch_id: int):
    url = "jdbc:mysql://127.0.0.1:3306"
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "db.wordCountsJson")
        .option("user", "root")
        .option("password", "password")
        .mode("append")
        .save()
    )

# Subscribe to wordCounts topic with offset at latest.
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "wordCounts")
    .option("startingOffsets", "latest")
    .load()
)

# Convert topic values and select required columns.
stream_df = (
    stream_df
    .withColumn("word", stream_df["key"].cast("string").alias("word"))
    .drop("key")
    .withColumn("count", stream_df["value"].cast("string").alias("count"))
    .drop("value")
)

# Convert to json
stream_df_json = (
    stream_df
    .withColumn("words_json", to_json(struct("word", "count", "timestamp")))
    .select("words_json")
)

# Checkpoint directory for running job.
checkpointDir = "./checkpoint_mysql"

# MySql Sink
stream_query = (
    stream_df_json
    .writeStream
    .outputMode("update")
    .foreachBatch(my_sql_sink)
    .option("checkpointLocation", checkpointDir)
    .trigger(processingTime="2 minutes")
    .start()
)

stream_query.awaitTermination()