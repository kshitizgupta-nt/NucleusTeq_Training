from datetime import datetime
import glob

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, get_current_context


def choose_format():
    context = get_current_context()
    conf = context["dag_run"].conf or {}
    file_format = str(conf.get("file_format", "")).strip().lower()
    source = conf.get("file_location")

    if not source:W
        raise ValueError("dag_run.conf.file_location is required")
    if not glob.glob(source):
        raise FileNotFoundError(f"No input files match: {source}")
    print(f"[AIRFLOW] Input pattern: {source}")
    print(f"[AIRFLOW] Format: {file_format}; execution_id: {conf.get('execution_id')}; expected_count: {conf.get('expected_count')}")

    if file_format == "csv":
        return "execute_csv_beam_etl"
    if file_format in {"json", "jsonl"}:
        return "execute_json_beam_etl"
    raise ValueError(f"Unsupported file format: {file_format}")


def beam_command(file_format):
    return (
        "set -euo pipefail; "
        "rm -rf /tmp/lumi_beam_run; "
        "mkdir -p /tmp/lumi_beam_run /opt/airflow/data_ingress/error_logs; "
        "cd /tmp/lumi_beam_run; "
        "jar -xf /opt/airflow/plugins/lumi-0.0.1-SNAPSHOT.jar; "
        "java -cp 'BOOT-INF/classes:BOOT-INF/lib/*' "
        "casestudy.lumi.beam.BeamProcessor "
        "'{{ dag_run.conf['file_location'] }}' "
        "'{{ dag_run.conf['execution_id'] }}' "
        f"'{file_format}' "
        "'{{ dag_run.conf.get('source_creation_time', '') }}' "
        '"jdbc:mysql://mysql:3306/capstone_lumi_dw" "lumi" "lumi_password" '
        "'/opt/airflow/data_ingress/error_logs'"
    )


with DAG(
    dag_id="amex_employee_ingestion",
    default_args={
        "owner": "amex_lumi",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "retries": 0,
    },
    description="Amex Lumi employee ingestion pipeline",
    schedule=None,
    catchup=False,
) as dag:
    verify_source_file = BashOperator(
        task_id="verify_source_file",
        bash_command=(
            "set -euo pipefail; "
            "source_pattern='{{ dag_run.conf.get('file_location', '') }}'; "
            "if [ -z \"$source_pattern\" ] || ! compgen -G \"$source_pattern\" > /dev/null; then "
            "echo \"Source files not found: $source_pattern\"; exit 1; fi; "
            "echo \"[AIRFLOW] Source files located: $source_pattern\"; "
            "find /opt/airflow/data_ingress -maxdepth 3 -type f -name 'part-*' -printf '%p %s bytes\\n'"
        ),
    )

    prepare_environment = BashOperator(
        task_id="prepare_environment",
        bash_command="set -euo pipefail; mkdir -p /opt/airflow/data_ingress/error_logs",
    )

    detect_file_type = BranchPythonOperator(
        task_id="detect_file_type",
        python_callable=choose_format,
    )

    execute_csv_beam_etl = BashOperator(
        task_id="execute_csv_beam_etl",
        bash_command=beam_command("csv"),
    )
    execute_json_beam_etl = BashOperator(
        task_id="execute_json_beam_etl",
        bash_command=beam_command("json"),
    )
    validate_record_count = BashOperator(
        task_id="validate_record_count",
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "set -euo pipefail; "
            "actual_count=$(mysql -h \"${LUMI_DB_HOST}\" -P \"${LUMI_DB_PORT}\" "
            "-u \"${LUMI_DB_USERNAME}\" -p\"${LUMI_DB_PASSWORD}\" -Nse "
            "\"SELECT COUNT(*) FROM ${LUMI_DB_NAME}.employees "
            "WHERE execution_id = '{{ dag_run.conf['execution_id'] }}'\"); "
            "expected_count='{{ dag_run.conf['expected_count'] }}'; "
            "echo \"Expected ingested record count: $expected_count\"; "
            "echo \"Actual ingested record count: $actual_count\"; "
            "mysql -h \"${LUMI_DB_HOST}\" -P \"${LUMI_DB_PORT}\" -u \"${LUMI_DB_USERNAME}\" "
            "-p\"${LUMI_DB_PASSWORD}\" -Nse \"SELECT employee_id, execution_id, ingestion_timestamp, source_creation_time "
            "FROM ${LUMI_DB_NAME}.employees WHERE execution_id = '{{ dag_run.conf['execution_id'] }}' LIMIT 3\"; "
            "if [ \"$actual_count\" -ne \"$expected_count\" ]; then "
            "echo 'Record count validation failed.'; exit 1; fi; "
            "echo 'Record count validation completed.'"
        ),
    )

    verify_source_file >> prepare_environment >> detect_file_type
    detect_file_type >> [
        execute_csv_beam_etl,
        execute_json_beam_etl,
    ]
    [
        execute_csv_beam_etl,
        execute_json_beam_etl,
    ] >> validate_record_count
