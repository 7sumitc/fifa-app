from pyspark.sql import functions as F

spark.sql("USE CATALOG fifa_worldcup")

bronze_df = spark.table("bronze.fixtures_raw")

silver_df = (
    bronze_df
    # Data types
    .withColumn(
        "year",
        F.col("year").cast("int")
    )
    .withColumn(
        "match_date",
        F.to_date(
            F.col("match_date"),
            "MM-dd-yyyy"
        )
    )
    .withColumn(
        "home_team_score",
        F.col("home_team_score").cast("int")
    )
    .withColumn(
        "away_team_score",
        F.col("away_team_score").cast("int")
    )
    .withColumn(
        "home_team_score_penalties",
        F.col("home_team_score_penalties").cast("int")
    )
    .withColumn(
        "away_team_score_penalties",
        F.col("away_team_score_penalties").cast("int")
    )
    .withColumn(
        "home_team_score_margin",
        F.col("home_team_score_margin").cast("int")
    )
    .withColumn(
        "away_team_score_margin",
        F.col("away_team_score_margin").cast("int")
    )
    # Match result
    .withColumn(
        "match_result",
        F.when(
            F.col("home_team_score") > F.col("away_team_score"),
            "H"
        )
        .when(
            F.col("home_team_score") < F.col("away_team_score"),
            "A"
        )
        .otherwise("D")
    )
    # Winner
    .withColumn(
        "winner",
        F.when(
            F.col("home_team_score") > F.col("away_team_score"),
            F.col("home_team")
        )
        .when(
            F.col("home_team_score") < F.col("away_team_score"),
            F.col("away_team")
        )
    )
    # Goal difference
    .withColumn(
        "goal_difference",
        F.abs(
            F.col("home_team_score") -
            F.col("away_team_score")
        )
    )
    # Penalties
    .withColumn(
        "went_to_penalties",
        F.when(
            (
                F.col("home_team_score_penalties") > 0
            ) |
            (
                F.col("away_team_score_penalties") > 0
            ),
            True
        ).otherwise(False)
    )
    # Knockout indicator
    .withColumn(
        "is_knockout",
        ~F.lower(F.col("stage")).like("%group%")
    )
    .withColumn(
        "fixture_id",
        F.md5(
            F.concat_ws(
                "|",
                F.col("year"),
                F.col("match_date"),
                F.col("home_team"),
                F.col("away_team")
            )
        )
    )
    .dropDuplicates()
)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS silver
""")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.fixtures")
)
