from fastapi import FastAPI

app = FastAPI(
    title="FIFA World Cup Predictor",
    version="1.0.0"
)

@app.get("/")
def home():
    return {
        "message": "FIFA World Cup Predictor"
    }

@app.get("/health")
def health():

    return {
        "status": "healthy"
    }
