from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "etl_data_gov"
TIMEZONE = pendulum.now(tz="America/Sao_Paulo")
PATH_DEFAULT = "/opt/airflow"
DAG_DIR = Path(__file__).parent
CONFIG_DIR = f"{PATH_DEFAULT}/configs"

SOURCES_FILE_NAME = "source.yaml"
SOURCE_CONFIG_FILE_PATH = f"{CONFIG_DIR}/{SOURCES_FILE_NAME}"
SOURCES = "PIPELINE"


@task(task_id="extract_data")
def extract_data(parameters: Dict, **kwargs) -> None:
    print(f"Reading file from {parameters['ENV']} env...")
    path_file = f'{parameters["INPUT_PATH"]}/{parameters["INPUT_FILE_NAME"]}'
    df = pd.read_csv(
        path_file, sep=";", encoding="latin", names=["code", "name"]
    )
    kwargs["ti"].xcom_push(
        key=f"{parameters['PIPELINE_NAME']}.raw_data", value=df.to_json()
    )
    kwargs["ti"].xcom_push(
        key=f"{parameters['PIPELINE_NAME']}.has_data",
        value=len(df.values.tolist()),
    )


@task.branch(task_id="check_has_data")
def check_has_data(pipeline_name: str, **kwargs) -> str:
    has_data = kwargs["ti"].xcom_pull(key=f"{pipeline_name}.has_data")
    print(f"Quantidade de dados {has_data}")
    if has_data > 0:
        return f"{pipeline_name}.transform_data"
    return f"{pipeline_name}.no_rows_found"


@task(task_id="transform_data")
def transform_data(parameters: Dict, **kwargs) -> None:
    records = kwargs["ti"].xcom_pull(
        key=f"{parameters['PIPELINE_NAME']}.raw_data"
    )
    df = pd.read_json(records)
    print("Transforming data...")
    df["group_name"] = df["name"].apply(
        lambda group_name: group_name.split(" ")[0]
    )
    df = df.groupby("group_name", as_index=False).agg(
        count_name=("group_name", "count")
    )
    kwargs["ti"].xcom_push(
        key=f"{parameters['PIPELINE_NAME']}.transformed_data",
        value=df.to_json(),
    )


@task(task_id="load_data")
def load_data(parameters: Dict, **kwargs) -> None:
    records = kwargs["ti"].xcom_pull(
        key=f"{parameters['PIPELINE_NAME']}.transformed_data"
    )
    df = pd.read_json(records)
    now = datetime.now().strftime("%Y_%m_%d")
    path_file = parameters["OUTPUT_PATH"]
    file_name = f'{parameters["OUTPUT_FILE_NAME"].format(today=now)}'
    print(f"Creating file {file_name}...")
    df["date"] = now
    df.to_csv(f"{path_file}/{file_name}", sep=";", index=False)


@dag(
    dag_id=DAG_ID,
    start_date=TIMEZONE,
    schedule_interval=None,
)
def create_dag():
    split_files_by_source = DummyOperator(task_id="ETL_GOV_DATA")
    end = DummyOperator(
        task_id="complete", trigger_rule=TriggerRule.NONE_FAILED
    )

    source_config_file_path = Path(SOURCE_CONFIG_FILE_PATH)

    sources = {}

    if source_config_file_path.exists():
        with open(source_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        sources = sources_config[SOURCES]["ENV"]

    for env, param in sources.items():
        with TaskGroup(group_id=param["PIPELINE_NAME"]) as task_group:
            parameters = {
                "ENV": env,
                "PIPELINE_NAME": param["PIPELINE_NAME"],
                "INPUT_PATH": param["INPUT_PATH"],
                "OUTPUT_PATH": param["OUTPUT_PATH"],
                "INPUT_FILE_NAME": param["INPUT_FILE_NAME"],
                "OUTPUT_FILE_NAME": param["OUTPUT_FILE_NAME"],
            }
            no_rows_found = DummyOperator(task_id="no_rows_found")
            extract = extract_data(parameters)
            transform = transform_data(parameters)
            load = load_data(parameters)
            has_data = check_has_data(parameters["PIPELINE_NAME"])

            extract >> has_data >> transform >> load
            extract >> has_data >> no_rows_found

        split_files_by_source >> task_group >> end


globals()[DAG_ID] = create_dag()
