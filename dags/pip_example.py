from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

DAG_ID = "etl_pj_br"

DAG_DIR = Path(__file__).parent
CONFIG_DIR = "/configs"

SOURCES_FILE_NAME = "sources.yaml"
SOURCE_CONFIG_FILE_PATH = f"{CONFIG_DIR}/{SOURCES_FILE_NAME}"
SOURCES = "sources"


@task(task_id="extract_data")
def extract(source, **kwargs):
    print("Reading file...")
    df = pd.read_csv(
        "/data/gov.csv", sep=";", encoding="latin", names=["code", "name"]
    )
    kwargs["ti"].xcom_push(key="raw_data", value=iter(df))


@task(task_id="transform_data")
def transform(source, **kwargs):
    df = next(kwargs["ti"].xcom_pull(key="raw_data", task_ids="extract"))
    print("Transforming data...")
    df["group_name"] = df["name"].apply(
        lambda group_name: group_name.split(" ")[0]
    )
    df = df.groupby("group_name").agg(count_name=("group_name", "count"))
    kwargs["ti"].xcom_push(key="transformed_data", value=iter(df))


@task(task_id="load_data")
def load(source, **kwargs):
    df = next(
        kwargs["ti"].xcom_pull(key="transformed_data", task_ids="transform")
    )
    now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    print(f"Creating file data_{now}.csv...")
    df.to_csv(f"/data/data_{now}.csv", sep=";", index=False)


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="America/Sao_Paulo"),
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="ETL_GOV_DATA")

    end = DummyOperator(task_id="end")

    source_config_file_path = Path(SOURCE_CONFIG_FILE_PATH)

    sources = []

    if source_config_file_path.exists():
        with open(source_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        sources = sources_config.get(SOURCES, [])

    for source in sources:
        with TaskGroup(group_id=source) as task_group:
            run_extract_data = extract(source)
            run_transform_data = transform(source)
            run_load_data = load(source)
            run_extract_data >> run_transform_data >> run_load_data

        split_files_by_source >> task_group >> end


globals()[DAG_ID] = create_dag()
