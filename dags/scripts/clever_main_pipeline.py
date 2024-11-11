import logging
import pandas as pd
from scripts.postgres_helper import upload_overwrite_table, run_sql

LOGGER = logging.getLogger("airflow.task")

def upload_to_postgres(**kwargs):
    """
    Uploads a CSV file to a PostgreSQL database, overwriting any existing data in the specified table.

    Parameters:
        kwargs (dict):
            - file_name (str): The name of the CSV file to upload.
            - table_name (str): The name of the target table in the PostgreSQL database.
    """
    file_name=kwargs.get("file_name")
    table_name = kwargs.get("table_name")

    raw_df = pd.read_csv(
        f"dags/scripts/data_examples/{file_name}", escapechar="\\"
    )

    LOGGER.info(f"uploading {table_name} to DB")

    upload_overwrite_table(raw_df, table_name)

def transform_data(**kwargs):
    """
    Executes a SQL query from a file to transform data in the PostgreSQL database.

    Parameters:
        kwargs (dict):
            - query_file (str): The filename of the SQL query to execute.
    """
    query_file = kwargs.get("query_file")

    query_path = f"queries/{query_file}"

    run_sql(query_path)
