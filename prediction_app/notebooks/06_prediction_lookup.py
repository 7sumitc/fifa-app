from itertools import product

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

spark.sql("USE CATALOG fifa_worldcup")

# ============================================================
# LOAD MODEL
# ============================================================

model = RandomForestClassificationModel.load(
    "/Volumes/fifa_worldcup/dataset/world-cup-data/rf_worldcup_model"
)

# ============================================================
# LOAD GOLD DATA
# ============================================================

gold_df = spark.table("gold.match_features")

# ============================================================
# BUILD TEAM LIST
# ============================================================

future_df = (
    spark.read
    .option("header", "true")
    .csv(
        "/Volumes/fifa_worldcup/dataset/world-cup-data/FIFA WC 2026.csv"
    )
)

teams_2026 = (
    future_df
    .select(
        F.col("home_team").alias("team")
    )
    .union(
        future_df.select(
            F.col("away_team").alias("team")
        )
    )
    .distinct()
)

team_list = [
    row["team"]
    for row in teams_2026.collect()
]

print(f"2026 Teams Found: {len(team_list)}")

# ============================================================
# CREATE ALL TEAM COMBINATIONS
# ============================================================

matchups = []

for home_team, away_team in product(
    team_list,
    team_list
):

    if home_team != away_team:

        matchups.append(
            (
                home_team,
                away_team,
                "Group Stage"
            )
        )

lookup_df = spark.createDataFrame(
    matchups,
    [
        "home_team",
        "away_team",
        "stage"
    ]
)

print(f"Matchups created: {lookup_df.count()}")

# ============================================================
# LATEST HOME FEATURES
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
# LATEST AWAY FEATURES
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
# JOIN FEATURES
# ============================================================

scoring_df = (
    lookup_df
    .join(
        latest_home,
        "home_team",
        "left"
    )
    .join(
        latest_away,
        "away_team",
        "left"
    )
)

# ============================================================
# STAGE ENCODING
# ============================================================

scoring_df = (
    scoring_df
    .withColumn(
        "stage_encoded",
        F.lit(1)
    )
)

# ============================================================
# H2H PLACEHOLDERS
# ============================================================

scoring_df = (
    scoring_df
    .withColumn(
        "h2h_matches_before",
        F.lit(0)
    )
    .withColumn(
        "h2h_home_wins_before",
        F.lit(0)
    )
    .withColumn(
        "h2h_away_wins_before",
        F.lit(0)
    )
    .withColumn(
        "h2h_draws_before",
        F.lit(0)
    )
)

# ============================================================
# NULL HANDLING
# ============================================================

scoring_df = scoring_df.fillna(0)

# ============================================================
# FEATURES
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
# SCORE
# ============================================================

predictions = model.transform(scoring_df)

# ============================================================
# MAP RESULT
# ============================================================

predictions = (
    predictions
    .withColumn(
        "predicted_result",
        F.when(
            F.col("prediction") == 0,
            "H"
        )
        .when(
            F.col("prediction") == 1,
            "D"
        )
        .otherwise("A")
    )
)

# ============================================================
# FINAL LOOKUP TABLE
# ============================================================

lookup_predictions = (
    predictions
    .select(
        "home_team",
        "away_team",
        "stage",
        "predicted_result"
    )
)

# ============================================================
# SAVE
# ============================================================

(
    lookup_predictions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.match_predictions_2026")
)

# ============================================================
# VALIDATION
# ============================================================

display(
    lookup_predictions
)

print(
    f"Lookup rows: {lookup_predictions.count()}"
)