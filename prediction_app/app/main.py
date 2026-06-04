from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {
        "message": "FIFA World Cup Predictor"
    }
