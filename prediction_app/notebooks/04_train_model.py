from pyspark.sql import functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark.sql("USE CATALOG fifa_worldcup")

df = spark.table("gold.match_features")

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

df = (
    df
    .fillna(0)
    .withColumn(
        "label",
        F.when(F.col("target_result") == "H", 0)
         .when(F.col("target_result") == "D", 1)
         .otherwise(2)
    )
)

train_df = df.filter(F.col("dataset_split") == "TRAIN")
test_df = df.filter(F.col("dataset_split") == "TEST")

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

train_df = assembler.transform(train_df)
test_df = assembler.transform(test_df)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    predictionCol="prediction",
    probabilityCol="probability",
    numTrees=200,
    maxDepth=8,
    seed=42
)

model = rf.fit(train_df)

predictions = model.transform(test_df)

accuracy = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
).evaluate(predictions)

f1_score = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
).evaluate(predictions)

print(f"Accuracy : {accuracy:.4f}")
print(f"F1 Score : {f1_score:.4f}")

display(
    predictions.select(
        "year",
        "stage",
        "home_team",
        "away_team",
        "target_result",
        "prediction",
        "probability"
    )
)

display(
    predictions.groupBy(
        "label",
        "prediction"
    ).count()
)

model_path = (
    "/Volumes/fifa_worldcup/"
    "dataset/world-cup-data/"
    "rf_worldcup_model"
)

model.write().overwrite().save(model_path)

print(f"Model saved to: {model_path}")

importance_df = spark.createDataFrame(
    [
        (feature_columns[i],
         float(model.featureImportances[i]))
        for i in range(len(feature_columns))
    ],
    ["feature", "importance"]
)

display(
    importance_df.orderBy(
        F.desc("importance")
    )
)