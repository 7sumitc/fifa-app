import os

from databricks.sdk import WorkspaceClient


def get_fixtures():
    try:
        w = WorkspaceClient()

        table_name = os.getenv("UC_TABLE_NAME")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

        print("TABLE: ", table_name)
        print("WAREHOUSE: ", warehouse_id)

        response = (
            w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"""
                    SELECT
                        home_team,
                        away_team
                    FROM {table_name}
                """,
                wait_timeout="30s"
            )
        )
        return [{
            "label": str(response),
            "home_team": "",
            "away_team": ""
        }]
    
    except Exception as e:
        return [str(e)]


def get_prediction(
    home_team: str,
    away_team: str
):
    try:
        w = WorkspaceClient()

        table_name = os.getenv("UC_TABLE_NAME")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        
        response = (
            w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"""
                    SELECT predicted_result
                    FROM {table_name}
                    WHERE home_team = '{home_team}'
                    AND away_team = '{away_team}'
                    LIMIT 1
                """,
                wait_timeout="30s"
            )
        )
        if not response.result:
            return "No Prediction"
        
        rows = response.result.data_array

        if not rows:
            return "No Prediction"
        
        result = rows[0][0]

        mapping = {
            "H": "🏠 Home Win",
            "D": "🤝 Draw",
            "A": "✈️ Away Win"
        }

        return mapping.get(
            result,
            result
        )
    except Exception as e:
        return str(e)
