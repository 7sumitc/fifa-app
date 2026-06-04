from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG fifa_worldcup")

silver_df = spark.table("silver.fixtures")

# ============================================================
# TEAM HISTORY
# ============================================================

home_history = (
    silver_df
    .select(
        "fixture_id",
        "year",
        "match_date",
        F.col("home_team").alias("team"),
        F.col("home_team_score").alias("goals_scored"),
        F.col("away_team_score").alias("goals_conceded"),
        F.when(F.col("match_result") == "H", 1).otherwise(0).alias("win"),
        F.when(F.col("match_result") == "D", 1).otherwise(0).alias("draw"),
        F.when(F.col("match_result") == "A", 1).otherwise(0).alias("loss")
    )
)

away_history = (
    silver_df
    .select(
        "fixture_id",
        "year",
        "match_date",
        F.col("away_team").alias("team"),
        F.col("away_team_score").alias("goals_scored"),
        F.col("home_team_score").alias("goals_conceded"),
        F.when(F.col("match_result") == "A", 1).otherwise(0).alias("win"),
        F.when(F.col("match_result") == "D", 1).otherwise(0).alias("draw"),
        F.when(F.col("match_result") == "H", 1).otherwise(0).alias("loss")
    )
)

team_history = home_history.unionByName(away_history)

team_history = (
    team_history
    .withColumn(
        "goal_diff",
        F.col("goals_scored") - F.col("goals_conceded")
    )
)

history_window = (
    Window
    .partitionBy("team")
    .orderBy("match_date")
    .rowsBetween(Window.unboundedPreceding, -1)
)

team_features = (
    team_history
    .withColumn(
        "matches_before",
        F.count("*").over(history_window)
    )
    .withColumn(
        "wins_before",
        F.coalesce(F.sum("win").over(history_window), F.lit(0))
    )
    .withColumn(
        "draws_before",
        F.coalesce(F.sum("draw").over(history_window), F.lit(0))
    )
    .withColumn(
        "losses_before",
        F.coalesce(F.sum("loss").over(history_window), F.lit(0))
    )
    .withColumn(
        "goals_scored_before",
        F.coalesce(F.sum("goals_scored").over(history_window), F.lit(0))
    )
    .withColumn(
        "goals_conceded_before",
        F.coalesce(F.sum("goals_conceded").over(history_window), F.lit(0))
    )
    .withColumn(
        "goal_diff_before",
        F.coalesce(F.sum("goal_diff").over(history_window), F.lit(0))
    )
)

team_features = (
    team_features
    .withColumn(
        "win_pct_before",
        F.when(
            F.col("matches_before") > 0,
            F.col("wins_before") / F.col("matches_before")
        ).otherwise(0.0)
    )
    .withColumn(
        "avg_goals_scored_before",
        F.when(
            F.col("matches_before") > 0,
            F.col("goals_scored_before") / F.col("matches_before")
        ).otherwise(0.0)
    )
    .withColumn(
        "avg_goals_conceded_before",
        F.when(
            F.col("matches_before") > 0,
            F.col("goals_conceded_before") / F.col("matches_before")
        ).otherwise(0.0)
    )
    .withColumn(
        "avg_goal_diff_before",
        F.when(
            F.col("matches_before") > 0,
            F.col("goal_diff_before") / F.col("matches_before")
        ).otherwise(0.0)
    )
)

# ============================================================
# HOME FEATURES
# ============================================================

home_features = (
    silver_df
    .select(
        "fixture_id",
        F.col("home_team").alias("team")
    )
    .join(
        team_features,
        ["fixture_id", "team"],
        "left"
    )
    .select(
        "fixture_id",
        F.col("matches_before").alias("home_matches_before"),
        F.col("wins_before").alias("home_wins_before"),
        F.col("draws_before").alias("home_draws_before"),
        F.col("losses_before").alias("home_losses_before"),
        F.col("goals_scored_before").alias("home_goals_scored_before"),
        F.col("goals_conceded_before").alias("home_goals_conceded_before"),
        F.col("goal_diff_before").alias("home_goal_diff_before"),
        F.col("win_pct_before").alias("home_win_pct_before"),
        F.col("avg_goals_scored_before").alias("home_avg_goals_scored_before"),
        F.col("avg_goals_conceded_before").alias("home_avg_goals_conceded_before"),
        F.col("avg_goal_diff_before").alias("home_avg_goal_diff_before")
    )
)

# ============================================================
# AWAY FEATURES
# ============================================================

away_features = (
    silver_df
    .select(
        "fixture_id",
        F.col("away_team").alias("team")
    )
    .join(
        team_features,
        ["fixture_id", "team"],
        "left"
    )
    .select(
        "fixture_id",
        F.col("matches_before").alias("away_matches_before"),
        F.col("wins_before").alias("away_wins_before"),
        F.col("draws_before").alias("away_draws_before"),
        F.col("losses_before").alias("away_losses_before"),
        F.col("goals_scored_before").alias("away_goals_scored_before"),
        F.col("goals_conceded_before").alias("away_goals_conceded_before"),
        F.col("goal_diff_before").alias("away_goal_diff_before"),
        F.col("win_pct_before").alias("away_win_pct_before"),
        F.col("avg_goals_scored_before").alias("away_avg_goals_scored_before"),
        F.col("avg_goals_conceded_before").alias("away_avg_goals_conceded_before"),
        F.col("avg_goal_diff_before").alias("away_avg_goal_diff_before")
    )
)

# ============================================================
# HEAD TO HEAD FEATURES
# ============================================================

h2h_base = (
    silver_df
    .select(
        "fixture_id",
        "match_date",
        "home_team",
        "away_team",
        "match_result"
    )
)

h2h_records = h2h_base.collect()

h2h_stats = []

for current_match in h2h_records:

    home_team = current_match["home_team"]
    away_team = current_match["away_team"]
    match_date = current_match["match_date"]
    fixture_id = current_match["fixture_id"]

    previous_matches = [
        row for row in h2h_records
        if row["match_date"] < match_date
        and (
            (
                row["home_team"] == home_team
                and row["away_team"] == away_team
            )
            or
            (
                row["home_team"] == away_team
                and row["away_team"] == home_team
            )
        )
    ]

    h2h_matches_before = len(previous_matches)
    h2h_home_wins_before = 0
    h2h_away_wins_before = 0
    h2h_draws_before = 0

    for match in previous_matches:

        if match["match_result"] == "D":
            h2h_draws_before += 1

        elif (
            match["home_team"] == home_team
            and match["match_result"] == "H"
        ) or (
            match["away_team"] == home_team
            and match["match_result"] == "A"
        ):
            h2h_home_wins_before += 1

        else:
            h2h_away_wins_before += 1

    h2h_stats.append(
        (
            fixture_id,
            h2h_matches_before,
            h2h_home_wins_before,
            h2h_away_wins_before,
            h2h_draws_before
        )
    )

h2h_df = spark.createDataFrame(
    h2h_stats,
    [
        "fixture_id",
        "h2h_matches_before",
        "h2h_home_wins_before",
        "h2h_away_wins_before",
        "h2h_draws_before"
    ]
)

# ============================================================
# STAGE ENCODING
# ============================================================

gold_df = (
    silver_df
    .join(home_features, "fixture_id", "left")
    .join(away_features, "fixture_id", "left")
    .join(h2h_df, "fixture_id", "left")

    .withColumn(
        "stage_encoded",
        F.when(F.col("stage") == "Group Stage", 1)
         .when(F.col("stage") == "Second Group Stage", 2)
         .when(F.col("stage") == "Round of 16", 3)
         .when(F.col("stage") == "Quarter Finals", 4)
         .when(F.col("stage") == "Semi Finals", 5)
         .when(F.col("stage") == "Third Place", 6)
         .when(F.col("stage") == "Final", 7)
         .when(F.col("stage") == "Final Round", 7)
         .otherwise(0)
    )

    .withColumnRenamed(
        "match_result",
        "target_result"
    )

    .withColumn(
        "dataset_split",
        F.when(F.col("year") < 2022, "TRAIN")
         .otherwise("TEST")
    )
)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS gold
""")

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.match_features")
)

display(
    spark.sql("""
    SELECT
        dataset_split,
        COUNT(1) AS matches
    FROM gold.match_features
    GROUP BY dataset_split
    ORDER BY dataset_split
    """)
)

display(
    spark.sql("""
    SELECT
        COUNT(1) total_rows,
        COUNT(DISTINCT fixture_id) unique_fixtures
    FROM gold.match_features
    """)
)