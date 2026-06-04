import os

from databricks.sdk import WorkspaceClient


def get_teams():

    w = WorkspaceClient()

    table_name = os.getenv("UC_TABLE_NAME")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"""
        SELECT DISTINCT home_team AS team
        FROM {table_name}

        UNION

        SELECT DISTINCT away_team AS team
        FROM {table_name}

        ORDER BY team
        """
    )

    rows = response.result.data_array

    return [row[0] for row in rows]


def get_prediction(
    home_team: str,
    away_team: str
):

    w = WorkspaceClient()

    table_name = os.getenv("UC_TABLE_NAME")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=f"""
        SELECT predicted_result
        FROM {table_name}
        WHERE home_team = '{home_team}'
          AND away_team = '{away_team}'
        LIMIT 1
        """
    )

    rows = response.result.data_array

    if not rows:
        return "No Prediction"

    result = rows[0][0]

    mapping = {
        "H": "Home Win",
        "D": "Draw",
        "A": "Away Win"
    }

    return mapping.get(
        result,
        result
    )