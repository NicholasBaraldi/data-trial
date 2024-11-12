from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.clever_main_pipeline import upload_to_postgres

default_args = {
    "owner": "nicholas.baraldi",
    "start_date": datetime(2024, 11, 5),
}

postgres_data = Dataset("http://localhost:8081/browser/")

datasets = [
    "fmcsa_complaints.csv",
    "fmcsa_safer_data.csv",
    "fmcsa_company_snapshot.csv",
    "fmcsa_companies.csv",
    "customer_reviews_google.csv",
    "company_profiles_google_maps.csv"
]

with DAG(
    dag_id="ingestion",
    default_args=default_args,
    catchup=False,
    schedule="20 0 * * *",
    max_active_runs=1
):

    start_task = EmptyOperator(task_id="Start")

    with TaskGroup("upload_datasets") as upload_group:

        for file in datasets:
            file_without_extension = file.split(".")[0]

            upload_to_postgres_task = PythonOperator(
                task_id=f"upload_to_postgres_{file_without_extension}",
                python_callable=upload_to_postgres,
                op_kwargs={
                    "file_name": file,
                    "table_name": file_without_extension
                },
                execution_timeout=timedelta(seconds=60)
            )

    finish_task = EmptyOperator(
        task_id="Finish",
        trigger_rule="none_failed",
        outlets=postgres_data
    )

    start_task >> upload_group >> finish_task
