from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Inc Load").getOrCreate()

# Define the initial data
initial_data = [
    (1, "Alice", "2024-08-01 10:00:00"),
    (2, "Bob", "2024-08-02 11:30:00"),
    (3, "Charlie", "2024-08-03 14:15:00"),
    (4, "David", "2024-08-04 09:45:00"),
    (5, "Eva", "2024-08-05 13:00:00")
]

#Create a DataFrame
columns = ["id", "name", "last_modified"]
df_initial = spark.createDataFrame(initial_data, columns)

# Write the initial data to a Delta table
df_initial.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/load")


df_initial=df_initial.withColumn("last_modified",col("last_modified").cast("timestamp"))

watermark_df = df_initial.selectExpr("max(last_modified) as max_last_modified")

# Write the watermark value to another Delta table
watermark_df.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark_table")


# Define the incremental data
incremental_data = [
    (2, "Bob Smith", "2024-08-06 11:30:00"),  # Updated record for id=2
    (5, "Eva", "2024-08-07 13:00:00"),        # Updated record for id=5
    (6, "Frank", "2024-08-07 15:00:00"),      # New record
    (7, "Grace", "2024-08-07 16:00:00")       # New record
]

# Create a DataFrame
df_incremental = spark.createDataFrame(incremental_data, columns)

df_incremental=df_incremental.withColumn("last_modified",col("last_modified").cast("timestamp"))

# Read the last watermark
last_load_timestamp = spark.read.format("delta").load("dbfs:/user/hive/warehouse/watermark_table").selectExpr("max(max_last_modified)").collect()[0][0]

# Filter the incremental data based on the last load timestamp
new_changes_df = df_incremental.filter(f"last_modified > '{last_load_timestamp}'")

# Load the Delta table
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/load")

# Merge new changes into the existing Delta table
deltaTable.alias("target").merge(
    new_changes_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Update the watermark with the new last_modified
new_last_modified = new_changes_df.selectExpr("max(last_modified) as max_last_modified").collect()[0][0]
new_last_modified
spark.createDataFrame([(new_last_modified,)], ["max_last_modified"] ).write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark_table")
