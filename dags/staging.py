from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.clever_main_pipeline import transform_data

default_args = {
    "owner": "nicholas.baraldi",
    "start_date": datetime(2024, 11, 5),
}

queries = [
    "customer_reviews_google.sql",
    "company_profiles_google_maps.sql"
]

with DAG(
    dag_id="staging_data",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 1 * * *",
    max_active_runs=1
) as dag:

    for query_file in queries:
        file_without_extension = query_file.split(".")[0]

        transform_data_task = PythonOperator(
            task_id=f"{file_without_extension}",
            python_callable=transform_data,
            op_kwargs={
                "query_file": query_file,
            },
            dag=dag
        )
