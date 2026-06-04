from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

spark.sql("USE CATALOG fifa_worldcup")

# ============================================================
# LOAD HISTORICAL FEATURES
# ============================================================

gold_df = spark.table("gold.match_features")

# ============================================================
# LOAD 2026 FIXTURES
# ============================================================

future_df = (
    spark.read
    .option("header", "true")
    .csv(
        "/Volumes/fifa_worldcup/dataset/world-cup-data/FIFA WC 2026.csv"
    )
)

# ============================================================
# LATEST HOME TEAM STATE
# ============================================================

home_window = (
    Window
    .partitionBy("home_team")
    .orderBy(F.desc("match_date"))
)

latest_home = (
    gold_df
    .withColumn(
        "rn",
        F.row_number().over(home_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "home_team",
        "home_matches_before",
        "home_wins_before",
        "home_draws_before",
        "home_losses_before",
        "home_goals_scored_before",
        "home_goals_conceded_before",
        "home_goal_diff_before",
        "home_win_pct_before",
        "home_avg_goals_scored_before",
        "home_avg_goals_conceded_before",
        "home_avg_goal_diff_before"
    )
)

# ============================================================
# LATEST AWAY TEAM STATE
# ============================================================

away_window = (
    Window
    .partitionBy("away_team")
    .orderBy(F.desc("match_date"))
)

latest_away = (
    gold_df
    .withColumn(
        "rn",
        F.row_number().over(away_window)
    )
    .filter(F.col("rn") == 1)
    .select(
        "away_team",
        "away_matches_before",
        "away_wins_before",
        "away_draws_before",
        "away_losses_before",
        "away_goals_scored_before",
        "away_goals_conceded_before",
        "away_goal_diff_before",
        "away_win_pct_before",
        "away_avg_goals_scored_before",
        "away_avg_goals_conceded_before",
        "away_avg_goal_diff_before"
    )
)

# ============================================================
# BUILD SCORING DATASET
# ============================================================

scoring_df = (
    future_df
    .join(latest_home, "home_team", "left")
    .join(latest_away, "away_team", "left")
)

# ============================================================
# STAGE ENCODING
# ============================================================

scoring_df = (
    scoring_df
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
         .otherwise(1)
    )
)

# ============================================================
# H2H PLACEHOLDERS
# ============================================================

scoring_df = (
    scoring_df
    .withColumn("h2h_matches_before", F.lit(0))
    .withColumn("h2h_home_wins_before", F.lit(0))
    .withColumn("h2h_away_wins_before", F.lit(0))
    .withColumn("h2h_draws_before", F.lit(0))
)

# ============================================================
# HANDLE NEW TEAMS
# ============================================================

scoring_df = scoring_df.fillna(0)

# ============================================================
# FEATURE LIST
# ============================================================

feature_columns = [
    "home_matches_before",
    "home_wins_before",
    "home_draws_before",
    "home_losses_before",
    "home_goals_scored_before",
    "home_goals_conceded_before",
    "home_goal_diff_before",
    "home_win_pct_before",
    "home_avg_goals_scored_before",
    "home_avg_goals_conceded_before",
    "home_avg_goal_diff_before",

    "away_matches_before",
    "away_wins_before",
    "away_draws_before",
    "away_losses_before",
    "away_goals_scored_before",
    "away_goals_conceded_before",
    "away_goal_diff_before",
    "away_win_pct_before",
    "away_avg_goals_scored_before",
    "away_avg_goals_conceded_before",
    "away_avg_goal_diff_before",

    "h2h_matches_before",
    "h2h_home_wins_before",
    "h2h_away_wins_before",
    "h2h_draws_before",

    "stage_encoded"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

scoring_df = assembler.transform(scoring_df)

# ============================================================
# LOAD MODEL
# ============================================================

model = RandomForestClassificationModel.load(
    "/Volumes/fifa_worldcup/dataset/world-cup-data/rf_worldcup_model"
)

# ============================================================
# SCORE
# ============================================================

predictions = model.transform(scoring_df)

# ============================================================
# MAP PREDICTIONS
# ============================================================

predictions = (
    predictions
    .withColumn(
        "predicted_result",
        F.when(F.col("prediction") == 0, F.lit("H"))
         .when(F.col("prediction") == 1, F.lit("D"))
         .otherwise(F.lit("A"))
    )
)

# ============================================================
# SAVE
# ============================================================

spark.sql("""
CREATE SCHEMA IF NOT EXISTS gold
""")

(
    predictions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.predictions_2026")
)

# ============================================================
# RESULTS
# ============================================================

group_stage_predictions = predictions.filter(
    F.col("stage") == "Group Stage"
)

display(
    group_stage_predictions.select(
        "home_team",
        "away_team",
        "predicted_result"
    )
)
