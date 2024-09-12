# incremental Data Load and Merge with Delta Lake
This script demonstrates an incremental data loading process using Delta Lake and PySpark. It performs the following steps:

1) Writes initial data to a Delta table.

2) Stores the last modification timestamp as a watermark to track data changes.

3) Loads incremental changes, filters them based on the last load timestamp, and merges them into the Delta table.

4) Updates the watermark after processing the new changes.

# Requirements
Apache Spark with PySpark and Delta Lake libraries.

Databricks File System (DBFS) or any other supported Delta Lake storage location.

# Script Overview
**Step 1**: Initialize Spark Session

    spark = SparkSession.builder.appName("Inc Load").getOrCreate()

**Step 2**: Load and Write Initial Data
    Initial data is defined and written to a Delta table. The data includes id, name, and last_modified timestamp columns.
    
    initial_data = [
        (1, "Alice", "2024-08-01 10:00:00"),
        (2, "Bob", "2024-08-02 11:30:00"),
        ...
    ]

    df_initial = spark.createDataFrame(initial_data, ["id", "name", "last_modified"])
    df_initial.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/load")

**Step 3**: Set and Store the Watermark
    The maximum last_modified timestamp from the initial data is used as the watermark to track the last load time. This value is stored in a separate Delta table.
    
    watermark_df = df_initial.selectExpr("max(last_modified) as max_last_modified")
    watermark_df.write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark_table")

**Step 4**: Define and Filter Incremental Data
New records or updated records are loaded as incremental data. The data is filtered using the last recorded last_modified value (watermark) to capture only new or modified records.

    incremental_data = [
        (2, "Bob Smith", "2024-08-06 11:30:00"),  # Updated record
        (5, "Eva", "2024-08-07 13:00:00"),        # Updated record
        (6, "Frank", "2024-08-07 15:00:00"),      # New record
        ...
    ]

    df_incremental = spark.createDataFrame(incremental_data, columns)
    last_load_timestamp = spark.read.format("delta").load("dbfs:/user/hive/warehouse/watermark_table").selectExpr("max(max_last_modified)").collect()[0][0]
    new_changes_df = df_incremental.filter(f"last_modified > '{last_load_timestamp}'")

**Step 5**: Merge Incremental Data into Delta Table
The filtered incremental data is merged into the existing Delta table using Delta Lake’s merge operation. This ensures that updated records are overwritten, and new records are inserted.

    from delta.tables import DeltaTable
    deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/load")

    deltaTable.alias("target").merge(
        new_changes_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

**Step 6**: Update the Watermark
After processing the new changes, the script updates the watermark with the latest last_modified value.
    
    new_last_modified = new_changes_df.selectExpr("max(last_modified) as max_last_modified").collect()[0][0]
    spark.createDataFrame([(new_last_modified,)], ["max_last_modified"]).write.format("delta").mode("overwrite").save("dbfs:/user/hive/warehouse/watermark_table")

# Usage Instructions
Initial Data Load: The script writes the initial dataset into a Delta table and stores the watermark value in a separate table.
Incremental Data Processing: Define the new or modified data and run the script to filter and merge incremental changes based on the last loaded timestamp.
Watermark Update: The script automatically updates the watermark table with the latest last_modified timestamp after processing new data.
# File Structure
Initial Delta Table: dbfs:/user/hive/warehouse/load
Watermark Table: dbfs:/user/hive/warehouse/watermark_table
