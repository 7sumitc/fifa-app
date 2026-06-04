from fastapi import FastAPI
from fastapi import Form
from fastapi import Request

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from fastapi.templating import Jinja2Templates

from predictor import get_prediction
from predictor import get_teams

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

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "teams": get_teams(),
            "prediction": None
        }
    )


@app.post(
    "/predict",
    response_class=HTMLResponse
)
async def predict(
    request: Request,
    home_team: str = Form(...),
    away_team: str = Form(...)
):

    prediction = get_prediction(
        home_team,
        away_team
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "teams": get_teams(),
            "home_team": home_team,
            "away_team": away_team,
            "prediction": prediction
        }
    )