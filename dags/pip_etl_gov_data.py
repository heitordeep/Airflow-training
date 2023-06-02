from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "etl_data_gov"

DAG_DIR = Path(__file__).parent
CONFIG_DIR = "/configs"

SOURCES_FILE_NAME = "sources.yaml"
SOURCE_CONFIG_FILE_PATH = f"{CONFIG_DIR}/{SOURCES_FILE_NAME}"
SOURCES = "sources"


@task(task_id="extract_data")
def extract_data(source, **kwargs):
    print("Reading file...")
    df = pd.read_csv(
        "/raw/gov.csv", sep=";", encoding="latin", names=["code", "name"]
    )
    kwargs["ti"].xcom_push(key=f"{source}.raw_data", value=df.to_json())
    kwargs["ti"].xcom_push(
        key=f"{source}.has_data", value=len(df.values.tolist())
    )


@task.branch(task_id="check_has_data")
def check_has_data(source, **kwargs):
    has_data = kwargs["ti"].xcom_pull(key=f"{source}.has_data")
    print("Quantidade de dados {}".format(has_data))
    if has_data > 0:
        return f"{source}.transform_data"
    return f"{source}.no_rows_found"


@task(task_id="transform_data")
def transform_data(source, **kwargs):
    records = kwargs["ti"].xcom_pull(key=f"{source}.raw_data")
    df = pd.read_json(records)
    print("Transforming data...")
    df["group_name"] = df["name"].apply(
        lambda group_name: group_name.split(" ")[0]
    )
    df = df.groupby("group_name", as_index=False).agg(
        count_name=("group_name", "count")
    )
    kwargs["ti"].xcom_push(
        key=f"{source}.transformed_data", value=df.to_json()
    )


@task(task_id="load_data")
def load_data(source, **kwargs):
    records = kwargs["ti"].xcom_pull(key=f"{source}.transformed_data")
    df = pd.read_json(records)
    now = datetime.now().strftime("%Y_%m_%d")
    print(f"Creating file data_{source}_{now}.csv...")
    df.to_csv(f"/trusted/dados_gov/data_{source}_{now}.csv", sep=";", index=False)


@dag(
    dag_id=DAG_ID,
    start_date=pendulum.now(tz="America/Sao_Paulo"),
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="ETL_GOV_DATA")
    end = DummyOperator(
        task_id="complete", trigger_rule=TriggerRule.NONE_FAILED
    )

    source_config_file_path = Path(SOURCE_CONFIG_FILE_PATH)

    sources = []

    if source_config_file_path.exists():
        with open(source_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        sources = sources_config.get(SOURCES, [])

    for source in sources:
        with TaskGroup(group_id=source) as task_group:
            no_rows_found = DummyOperator(task_id="no_rows_found")
            has_data = check_has_data(source)
            extract = extract_data(source)
            transform = transform_data(source)
            load = load_data(source)

            extract >> has_data >> transform >> load
            extract >> has_data >> no_rows_found

        split_files_by_source >> task_group >> end


globals()[DAG_ID] = create_dag()
