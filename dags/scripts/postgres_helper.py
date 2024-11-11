import logging
import pandas as pandas
from sqlalchemy import create_engine, text
import scripts.constants as c

LOGGER = logging.getLogger("airflow.task")

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}"
)

def run_sql(path: str) -> None:
    """
    Executes an SQL query stored in a specified file.

    Parameters:
        path (str): The file path of the SQL query to execute.
    """
    with open(path) as f:
        with engine.connect() as conn:
            conn.execute(text(f.read()))
            conn.close()

def upload_overwrite_table(df: pandas.DataFrame, table_name: str) -> None:
    """
    Uploads a pandas DataFrame to a specified table in a PostgreSQL database, overwriting the existing data.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing data to upload.
        table_name (str): The name of the target table in the PostgreSQL database.

    Notes:
        If the specified table exists, it will be replaced by the DataFrame data.
    """
    df.to_sql(f"{table_name}", engine, index=False, if_exists="replace")