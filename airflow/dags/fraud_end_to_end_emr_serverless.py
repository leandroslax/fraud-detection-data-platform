from __future__ import annotations

from datetime import timedelta
import re

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_REGION = Variable.get("AWS_REGION", default_var="us-east-1")
LAKE_BUCKET = Variable.get("LAKE_BUCKET", default_var="fraud-datalake-dev-use1-139961319000")

EMR_SERVERLESS_APP_ID = Variable.get("EMR_SERVERLESS_APP_ID", default_var="00g3n7hfntj7im09")
EMR_SERVERLESS_EXEC_ROLE_ARN = Variable.get(
    "EMR_SERVERLESS_EXECUTION_ROLE_ARN",
    default_var="arn:aws:iam::139961319000:role/EMRServerlessJobRole-FraudLake",
)

JOBS_PREFIX = Variable.get("JOBS_PREFIX", default_var="jobs/spark")

EMR_LOG_S3_URI = Variable.get(
    "EMR_LOG_S3_URI",
    default_var=f"s3://{LAKE_BUCKET}/emr-serverless-logs/",
)

BRONZE_TO_SILVER_ENTRYPOINT = f"s3://{LAKE_BUCKET}/{JOBS_PREFIX}/bronze_to_silver_fraud_events.py"
SILVER_TO_GOLD_ENTRYPOINT = f"s3://{LAKE_BUCKET}/{JOBS_PREFIX}/silver_to_gold_fraud_events.py"

# -------------------------------------------------------------------
# Spark params (CAPADOS para caber na quota de 16 vCPUs concorrentes)
# - Bronze->Silver: driver 1 vCPU + 6 executores x 2 vCPU = 13 vCPU
# - Silver->Gold : driver 1 vCPU + 4 executores x 2 vCPU = 9 vCPU
# -------------------------------------------------------------------

SPARK_PARAMS_BRONZE_TO_SILVER = " ".join(
    [
        "--conf spark.dynamicAllocation.enabled=false",
        "--conf spark.executor.instances=6",
        "--conf spark.executor.cores=2",
        "--conf spark.executor.memory=4g",
        "--conf spark.executor.memoryOverhead=2g",
        "--conf spark.driver.cores=1",
        "--conf spark.driver.memory=2g",
        "--conf spark.sql.shuffle.partitions=48",
        "--conf spark.default.parallelism=48",
        "--conf spark.sql.adaptive.enabled=true",
        "--conf spark.sql.adaptive.skewJoin.enabled=true",
    ]
)

SPARK_PARAMS_SILVER_TO_GOLD = " ".join(
    [
        "--conf spark.dynamicAllocation.enabled=false",
        "--conf spark.executor.instances=4",
        "--conf spark.executor.cores=2",
        "--conf spark.executor.memory=4g",
        "--conf spark.executor.memoryOverhead=2g",
        "--conf spark.driver.cores=1",
        "--conf spark.driver.memory=2g",
        "--conf spark.sql.shuffle.partitions=32",
        "--conf spark.default.parallelism=32",
        "--conf spark.sql.adaptive.enabled=true",
    ]
)


def _get_emr_serverless_client(region: str):
    import boto3

    return boto3.client("emr-serverless", region_name=region)


def ensure_emr_serverless_started(application_id: str, region: str, **_):
    client = _get_emr_serverless_client(region)
    state = client.get_application(applicationId=application_id)["application"].get("state")

    if state != "STARTED":
        client.start_application(applicationId=application_id)

    import time

    for _ in range(60):
        state = client.get_application(applicationId=application_id)["application"].get("state")
        if state == "STARTED":
            return
        time.sleep(10)

    raise TimeoutError(f"EMR Serverless application {application_id} did not reach STARTED (last={state}).")


def stop_emr_serverless(application_id: str, region: str, **_):
    client = _get_emr_serverless_client(region)
    try:
        state = client.get_application(applicationId=application_id)["application"].get("state")
        if state == "STARTED":
            client.stop_application(applicationId=application_id)
    except Exception as e:
        print(f"[WARN] Could not stop EMR Serverless application: {e}")


local_tz = pendulum.timezone("America/Sao_Paulo")

# Ajuste: menos retries e retry_delay maior pra evitar tempestade de tentativas
default_args = {
    "owner": "leandro",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="fraud_end_to_end_bronze_silver_gold_emr_serverless",
    description="End-to-End Fraud Data Pipeline (Bronze->Silver->Gold) on EMR Serverless",
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["fraud", "emr-serverless", "kinesis", "firehose", "s3", "spark"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start_emr_serverless_application = PythonOperator(
        task_id="start_emr_serverless_application",
        python_callable=ensure_emr_serverless_started,
        op_kwargs={"application_id": EMR_SERVERLESS_APP_ID, "region": AWS_REGION},
    )

    wait_bronze_stream_partition = S3KeySensor(
        task_id="wait_bronze_stream_partition",
        bucket_name=LAKE_BUCKET,
        bucket_key=(
            "bronze/source=stream/entity=fraud_events/"
            "year={{ ds_nodash[0:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/*.gz"
        ),
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    emr_bronze_to_silver_start = EmrServerlessStartJobOperator(
        task_id="emr_bronze_to_silver_start",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EMR_SERVERLESS_EXEC_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": BRONZE_TO_SILVER_ENTRYPOINT,
                "entryPointArguments": ["--ds", "{{ ds }}", "--bucket", LAKE_BUCKET],
                "sparkSubmitParameters": SPARK_PARAMS_BRONZE_TO_SILVER,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": EMR_LOG_S3_URI},
            },
            "applicationConfiguration": [
                {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.hadoop.hive.metastore.client.factory.class": (
                            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        )
                    },
                }
            ],
        },
        region_name=AWS_REGION,
    )

    wait_silver_any_output = S3KeySensor(
        task_id="wait_silver_any_output",
        bucket_name=LAKE_BUCKET,
        bucket_key="silver/fraud_events/**/*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    # -----------------------------
    # resolve last Silver partition date (ds) from S3
    # -----------------------------
    def _resolve_latest_silver_ds(**context):
        """
        Finds the most recent partition in:
        silver/fraud_events/event_year=YYYY/event_month=MM/event_day=DD/
        and returns ds as YYYY-MM-DD.
        """
        hook = S3Hook(aws_conn_id="aws_default")
        keys = hook.list_keys(bucket_name=LAKE_BUCKET, prefix="silver/fraud_events/") or []

        pat = re.compile(
            r"silver/fraud_events/event_year=(\d{4})/event_month=(\d{2})/event_day=(\d{2})/"
        )

        parts = set()
        for k in keys:
            m = pat.search(k)
            if m:
                y, mo, d = m.groups()
                parts.add((int(y), int(mo), int(d)))

        if not parts:
            raise ValueError("Nenhuma partição encontrada em silver/fraud_events/")

        y, mo, d = max(parts)
        latest_ds = f"{y:04d}-{mo:02d}-{d:02d}"

        context["ti"].xcom_push(key="latest_silver_ds", value=latest_ds)
        return latest_ds

    resolve_latest_silver_ds = PythonOperator(
        task_id="resolve_latest_silver_ds",
        python_callable=_resolve_latest_silver_ds,
    )

    emr_silver_to_gold_start = EmrServerlessStartJobOperator(
        task_id="emr_silver_to_gold_start",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EMR_SERVERLESS_EXEC_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": SILVER_TO_GOLD_ENTRYPOINT,
                "entryPointArguments": [
                    "--ds",
                    "{{ ti.xcom_pull(task_ids='resolve_latest_silver_ds', key='latest_silver_ds') }}",
                    "--bucket",
                    LAKE_BUCKET,
                    "--top-n",
                    "20",
                ],
                "sparkSubmitParameters": SPARK_PARAMS_SILVER_TO_GOLD,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": EMR_LOG_S3_URI},
            },
            "applicationConfiguration": [
                {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.hadoop.hive.metastore.client.factory.class": (
                            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        )
                    },
                }
            ],
        },
        region_name=AWS_REGION,
    )

    wait_gold_any_kpis_output = S3KeySensor(
        task_id="wait_gold_any_kpis_output",
        bucket_name=LAKE_BUCKET,
        bucket_key="gold/fraud_events_kpis/**/*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    stop_emr_serverless_application = PythonOperator(
        task_id="stop_emr_serverless_application",
        python_callable=stop_emr_serverless,
        op_kwargs={"application_id": EMR_SERVERLESS_APP_ID, "region": AWS_REGION},
        trigger_rule="all_done",
    )

    (
        start
        >> start_emr_serverless_application
        >> wait_bronze_stream_partition
        >> emr_bronze_to_silver_start
        >> wait_silver_any_output
        >> resolve_latest_silver_ds
        >> emr_silver_to_gold_start
        >> wait_gold_any_kpis_output
        >> end
        >> stop_emr_serverless_application
    )