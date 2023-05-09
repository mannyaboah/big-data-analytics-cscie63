"""🐍 __python__ script to perform upsert as per problem 3
"""
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip, DeltaTable

findspark.init()

# Output directory
OUTPUT_PATH = "output_data/test-table"

# Create a builder with the Delta extensions
builder = (
    SparkSession.builder.appName("delta_lake_app")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

# Create a Spark instance with the builder
# As a result, we now can read and write Delta files
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create a Delta Table
delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)

# Show table prior to upsert
print("Test Delta Table prior to upsert")
delta_table.toDF().show()

# Upsert/ merge the numbers 16 - 20
new_data = spark.range(20, 41)

(
    delta_table.alias("prev_data")
    .merge(new_data.alias("new_data"), "prev_data.id = new_data.id")
    .whenMatchedUpdate(set={"id": col("new_data.id")})
    .whenNotMatchedInsert(values={"id": col("new_data.id")})
    .execute()
)

# Show updated data
print("Updated Delta Table")

delta_table.toDF().show()
