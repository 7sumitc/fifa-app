# Real-time FIFA World Cup 2026 Dashboard using Databricks & Streamlit

A complete data engineering and machine learning project to track, analyze, and predict FIFA World Cup 2026 matches in real-time.

## 🚀 Features

- **Real-time Data Ingestion**: Fetches latest match data (fixtures, results, odds) every 30 seconds
- **Multi-Stage Medallion Architecture**: 
  - Bronze → Raw ingestion from API
  - Silver → Cleaned, typed data
  - Gold → Aggregated features for ML
- **Machine Learning**: Pre-trained XGBoost model for match win probability
- **Live Dashboard**: Interactive Streamlit UI with real-time updates
- **CI/CD Ready**: GitHub Actions workflow for automated deployments

## 📂 Project Structure

```
fifa-app/
├── src/
│   ├── databricks/                 # Databricks notebooks
│   │   ├── bronze_fixtures.py
│   │   ├── silver_fixtures.py
│   │   └── gold_features.py
│   ├── ingestion/                  # Data ingestion scripts
│   │   ├── api_client.py
│   │   └── stream_fixtures.py
│   ├── modeling/                   # ML pipelines
│   │   ├── train_model.py
│   │   └── predict.py
│   ├── orchestration/            # Workflow orchestration
│   │   └── pipeline.py
│   ├── streamlit/                  # Streamlit dashboard
│   │   └── app.py
│   └── utils/                      # Utility functions
│       └── spark_utils.py
├── config/
│   ├── settings.py
│   └── environment.py
├── data/
│   ├── raw/                        # Bronze layer
│   └── processed/                  # Silver/Gold layers
├── tests/
│   ├── test_api_client.py
│   └── test_pipeline.py
├── notebooks/                      # Exploration notebooks
├── .env                            # Environment variables
├── requirements.txt                # Python dependencies
└── README.md
```

## ⚙️ Setup

### Prerequisites
- Python 3.8+
- Databricks workspace
- AWS account (for S3 storage)

### Installation

1. Clone the repository:
```bash
git clone <repo-url>
cd fifa-app
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env

# Edit .env with your API keys and AWS credentials
```

## 🔌 Databricks Setup

### Option 1: Run notebooks directly

1. Upload notebooks to your Databricks workspace
2. Create a cluster with Python 3.10 and Spark 3.5+
3. Run notebooks in order:
   - Bronze: `bronze_fixtures.py`
   - Silver: `silver_fixtures.py`
   - Gold: `gold_features.py`

### Option 2: Deploy with CI/CD

1. Configure GitHub Actions secrets:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `DATABRICKS_CLUSTER_ID`
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION`

2. Trigger deployment:
```bash
git push origin main
```

## 📊 Real-time Dashboard

Run the Streamlit app:
```bash
streamlit run src/streamlit/app.py
```

The dashboard displays:
- Live match scores (from API)
- ML predictions (from model)
- Historical trends
- Data quality metrics

## 🧠 Machine Learning

### Training
Train the XGBoost model:
```bash
python src/modeling/train_model.py
```

The model is saved to `data/models/` and used for predictions.

### Inference
Predictions are automatically loaded from the trained model when the dashboard starts.

## 🧪 Testing

Run unit tests:
```bash
pytest tests/
```

## 🎯 CI/CD Pipeline

The GitHub Actions workflow automatically:
1. Validates code and notebooks
2. Runs tests
3. Deploys to Databricks
4. Syncs data to S3
5. Restarts the Streamlit app

## 📁 Data Flow

```
API (SportsDataIO) → Bronze (Raw JSON) → Silver (Cleaned) → Gold (Features) → Dashboard
                                                    ↓
                                                  Model
```

## 🛠️ Technology Stack

- **Data Processing**: PySpark, Databricks
- **Real-time**: Streamlit, WebSockets
- **Storage**: Delta Lake, S3
- **ML**: XGBoost, Scikit-learn
- **Orchestration**: GitHub Actions
- **APIs**: SportsDataIO

## 🔐 Environment Variables

Create a `.env` file in the project root:
```
# Databricks
DATABRICKS_HOST=https://dbc-...
DATABRICKS_TOKEN=...
DATABRICKS_CLUSTER_ID=...

# AWS
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1

# API Keys
API_KEY=...
```

## 🎯 Sample Notebooks

### Bronze (Ingestion)
```python
from databricks.sdk.runtime import spark

df = spark.read.format("delta").load("/data/bronze/fixtures")
display(df)
```

### Silver (Transformation)
```python
df = spark.sql("""
    SELECT 
        id,
        fixture_date,
        home_team,
        away_team,
        score
    FROM bronze.fixtures
    WHERE status = 'FT'
""")
```

### Gold (Features)
```python
df = spark.sql("""
    SELECT
        feature_vector,
        target,
        fixture_id
    FROM gold.features
""")
```

### Streamlit Dashboard
```python
import streamlit as st
import pandas as pd

df = pd.read_delta("/data/gold/features")
st.dataframe(df)
```

## 🤝 Contributing

1. Create a feature branch
2. Make your changes
3. Run tests
4. Submit a pull request

## 📝 License

This project is licensed under the MIT License.

## 📞 Support

For issues or questions, please open an issue in the repository.
