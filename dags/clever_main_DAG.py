from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.clever_main_pipeline import upload_to_postgres

default_args = {
    "owner": "nicholas.baraldi",
    "start_date": datetime(2024, 11, 5),
}

datasets = [
    "fmcsa_complaints.csv",
    "fmcsa_safer_data.csv",
    "fmcsa_company_snapshot.csv",
    "fmcsa_companies.csv",
    "customer_reviews_google.csv",
    "company_profiles_google_maps.csv"
]

with DAG(
    dag_id="clever_main_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval="20 0 * * *",
    max_active_runs=1
) as dag:

    start_task = EmptyOperator(task_id="Start", dag=dag)
    finish_task = EmptyOperator(task_id="Finish", trigger_rule="none_failed", dag=dag)

    with TaskGroup("datasets") as group:

        for file in datasets:
            file_without_extension = file.split(".")[0]

            upload_to_postgres_task = PythonOperator(
                task_id=f"upload_to_postgres_{file_without_extension}",
                python_callable=upload_to_postgres,
                op_kwargs={
                    "file_name": file,
                    "table_name": file_without_extension
                },
                dag=dag
            )

    start_task >> group >> finish_task
