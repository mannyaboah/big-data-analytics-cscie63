from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col

scala_version = '2.12'
spark_version = '3.3.1'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1'
]

spark = (
    SparkSession.builder
    .appName("StructuredWordCount")
    .config("spark.jars.packages", ",".join(packages))
    .getOrCreate()
)

lines = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999).load()
)

# Split the lines into words
words = lines.select(split(col("value"), "\\s").alias("word"))

# Generate running word count
counts: DataFrame = words.groupBy("word").count()
checkpointDir = "./checkpoint2"

# Sink counts streams to a kafka topic
streamingQuery = (
    counts.selectExpr("cast(word as string) as key", "cast(count as string) as value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "wordCounts")
    .outputMode("update")
    .option("checkpointLocation", checkpointDir)
    .trigger(processingTime="5 seconds")
    .start()
)

streamingQuery.awaitTermination()