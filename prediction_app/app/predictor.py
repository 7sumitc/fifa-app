import os

from databricks.sdk import WorkspaceClient


def get_teams():
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
                    SELECT DISTINCT team
                    FROM (
                        SELECT home_team AS team
                        FROM {table_name}
                        UNION
                        SELECT away_team AS team
                        FROM {table_name}
                    )
                    ORDER BY team
                """,
                wait_timeout="30s"
            )
        )
        if not response.result:
            return "No Data"
        
        rows = response.result.data_array
        return [row[0] for row in rows]
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
        return {
            "H": "Home Win",
            "D": "Draw",
            "A": "Away Win"
        }.get(
            result,
            result
        )
    except Exception as e:
        return str(e)
