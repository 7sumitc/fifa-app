PREDICTIONS = {
    ("Argentina", "France"): "H",
    ("Brazil", "England"): "H",
    ("Mexico", "USA"): "D"
}

TEAMS = [
    "Argentina",
    "Brazil",
    "England",
    "France",
    "Mexico",
    "USA"
]


def get_teams():
    return sorted(TEAMS)


def get_prediction(home_team, away_team):

    if home_team == away_team:
        return "Choose different teams"

    prediction = PREDICTIONS.get(
        (home_team, away_team),
        "H"
    )

    mapping = {
        "H": "Home Win",
        "D": "Draw",
        "A": "Away Win"
    }

    return mapping[prediction]