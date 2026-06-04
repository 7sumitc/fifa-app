from fastapi import FastAPI
from fastapi import Form
from fastapi import Request

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from predictor import get_fixtures
from predictor import get_prediction

app = FastAPI(
    title="FIFA World Cup Predictor"
)

app.mount(
    "/static",
    StaticFiles(directory="static"),
    name="static"
)
templates = Jinja2Templates(
    directory="templates"
)


@app.get(
    "/",
    response_class=HTMLResponse
)
async def home(
    request: Request
):
    fixtures = get_fixtures()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "fixtures": fixtures,
            "prediction": None
        }
    )


@app.post(
    "/predict",
    response_class=HTMLResponse
)
async def predict(
    request: Request,
    fixture: str = Form(...)
):

    home_team, away_team = fixture.split(
        "|||"
    )

    prediction = get_prediction(
        home_team,
        away_team
    )

    fixtures = get_fixtures()

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "fixtures": fixtures,
            "home_team": home_team,
            "away_team": away_team,
            "prediction": prediction
        }
    )


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }
