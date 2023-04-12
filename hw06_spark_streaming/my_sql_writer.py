from pyspark.sql import SparkSession, DataFrame

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

'''
    User defined function to write streams to MySql in foreachBatch
'''
def my_sql_sink(df: DataFrame, batch_id: int):
    url = "jdbc:mysql://127.0.0.1:3306"
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "db.wordCounts")
        .option("user", "root")
        .option("password", "password")
        .mode("append")
        .save()
    )

# Subscribe to 
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "wordCounts")
    .load()
    .selectExpr("CAST(key AS STRING) as word", "CAST(value AS STRING) as count")
)

checkpointDir = "./checkpoint_mysql"

stream_query = (
    stream_df
    .writeStream
    .outputMode("update")
    .foreachBatch(my_sql_sink)
    .option("checkpointLocation", checkpointDir)
    .trigger(processingTime="5 seconds")
    .start()
)

stream_query.awaitTermination()