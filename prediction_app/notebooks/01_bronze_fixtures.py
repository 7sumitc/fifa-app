from pyspark.sql import functions as F

spark.sql("USE CATALOG fifa_worldcup")

raw_df = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "false")
    .csv(
        "/Volumes/fifa_worldcup/dataset/world-cup-data/FIFA WC 1930 2022.csv"
    )
)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS bronze
""")

spark.sql("""
DROP TABLE IF EXISTS bronze.fixtures_raw
""")

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze.fixtures_raw")
)

display(
    spark.sql("""
    SELECT
        match_date
    FROM bronze.fixtures_raw
    LIMIT 10
    """)
)