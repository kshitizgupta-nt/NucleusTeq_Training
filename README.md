# Amex Lumi Case Study - Complete Project

This repository implements all three phases described in the supplied Amex Lumi case-study requirement. The design uses Spring Boot REST, Apache Beam, Apache Airflow, PySpark and MySQL. BigQuery/GCP is not required for local execution because the requirement explicitly permits local PostgreSQL/MySQL.

## Requirement coverage

### Phase 1
- Spring Boot REST endpoint accepts `file_location` and `control_file_location`.
- Spring Boot generates one UUID execution ID per ingestion request.
- Spring Boot triggers the Airflow REST API and passes all required DAG parameters.
- Apache Beam reads and parses JSON/JSONL and CSV.
- Missing/null values are converted to whitespace (numeric salary defaults to `0`, boolean `is_active` defaults to `true`).
- Every loaded row receives `ingestion_timestamp`, the same `execution_id`, and the source file creation timestamp.
- The three metadata columns are `ingestion_timestamp`, `execution_id`, and `source_creation_time`; nested `skills`, `address`, and `emergency_contact` values are stored as MySQL JSON.
- `phone_number`, `salary`, and `emergency_contact.phone` are stored as standard Base64 text; salary is therefore stored in a text column rather than as an integer.
- Bad input rows are written to `data_ingress/error_logs/error_records.txt` and do not stop valid rows from loading.
- Each error line is JSON with `execution_id`, UTC `timestamp`, `employee_id`, `reason`, and `raw_record`.
- Employee IDs are treated as unique business keys; records already present in the warehouse are rejected as duplicates and written to the error log instead of being inserted again.
- Individual database row-load errors are also written to the error file without aborting other valid rows.
- Airflow owns the orchestration.

### Phase 2
- Spring Boot reads the configurable file-size threshold.
- Files larger than the threshold invoke `data_ingress/file_splitter.py`.
- The splitter partitions JSON/JSONL and CSV files into parallel chunks.
- Split output is written as Spark `part-*` files and passed to the Airflow DAG.
- The supplied demo threshold is 200 bytes so the Phase 2 path is easy to demonstrate.

### Phase 3
- API reads `record_count` from the supplied control properties file.
- Expected count is passed to Airflow with the execution ID.
- The final Airflow task counts rows loaded for that execution ID.
- Expected and actual counts are printed.
- A mismatch fails the DAG; a match allows the workflow to complete.

## Project layout

```text
Amex_lumi_case_study/
├── build_project.bat
├── run_splitter.bat
├── data_ingress/
│   ├── demo_data.jsonl
│   ├── control.properties
│   ├── schema.sql
│   ├── file_splitter.py
│   ├── requirements.txt
│   └── error_logs/error_records.txt
├── lumi/
│   ├── pom.xml
│   ├── mvnw
│   ├── mvnw.cmd
│   └── src/
└── airflow_workspace/
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── .env
    └── dags/amex_ingestion_dag.py
```

## Prerequisites on the execution machine

- Java 17
- Python 3.10+
- Maven wrapper included in `lumi` (Maven itself is not required)
- Docker Desktop with WSL2/Linux containers working
- Python package: PySpark 3.5.6 (matching the local Spark launcher)

Install PySpark from the project root: `python -m pip install -r data_ingress/requirements.txt`.

## Build the Spring Boot/Beam JAR

From the project root run:

```bat
build_project.bat
```

Or from `lumi`:

```bat
mvnw.cmd clean package
```

The resulting JAR is `lumi/target/lumi-0.0.1-SNAPSHOT.jar`.

## Start Airflow and MySQL

From `airflow_workspace`:

```bat
docker compose up -d --build postgres mysql airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow UI: `http://localhost:8081`

Airflow credentials: `airflow / airflow`

MySQL is supplied as part of the Compose project, so a separate MySQL installation is not required. It is exposed on host port `3307` and is available to Airflow internally as `mysql:3306`. The employee table is created automatically from `data_ingress/schema.sql`.

## Start Spring Boot

Open the `lumi` directory in IntelliJ and run `LumiApplication`, or from the `lumi` directory run:

```bat
mvnw.cmd spring-boot:run
```

If IntelliJ uses the project root as the working directory, update the relative paths in `application.properties` to match your working directory. The provided defaults assume the application runs with `lumi` as its working directory.

## API request

`POST http://localhost:8080/api/v1/ingestion/trigger`

```json
{
  "file_location": "../data_ingress/demo_data.jsonl",
  "control_file_location": "../data_ingress/control.properties"
}
```

The API checks the requested files, reads the expected count, generates the execution ID, invokes PySpark when the threshold is exceeded, and triggers the Airflow DAG with the mounted input pattern, format, execution ID, and expected count.

## Demo behavior

`demo_data.jsonl` contains 20 records in a JSON array, including two intentionally invalid records. Therefore `control.properties` contains `record_count=18`, and Phase 3 should pass with expected=18 and actual=18.

To demonstrate a Phase 3 mismatch, temporarily change the control file to `record_count=21`. The ingestion rows still process, but the final Airflow validation task must fail with expected=21 and actual=20.

## Testing

Run from `lumi`:

```bat
mvnw.cmd test
```

Tests cover context startup, JSON objects/arrays/NDJSON, CSV quoted fields and malformed values, whitespace defaults, control-file validation, threshold behavior and JSON/CSV format detection.

## Manual PySpark test

After installing PySpark, from the project root:

```bat
run_splitter.bat
```

The splitter creates `data_ingress/split_chunks/part-*` and `_SUCCESS`. The `split_chunks` directory is runtime output. Airflow receives this same mounted path as `/opt/airflow/data_ingress/split_chunks/part-*`. The API passes its execution ID into the splitter so splitter errors use the same run ID.

## Security/configuration notes

Database and Airflow credentials are configuration values, not Java source constants. For a real deployment, replace the demo credentials in `airflow_workspace/.env` and use a secrets manager.

## JSON and CSV focus

The supported formats are JSON/JSONL and CSV. JSON supports a single object, a JSON array, and newline-delimited JSON (NDJSON). A malformed NDJSON record is retained in `data_ingress/error_logs/error_records.txt` by the splitter while valid records continue. CSV supports quoted fields, including commas and escaped quotes, blank values, and the supplied header format. A non-numeric salary is rejected as a bad row while other rows continue.

CSV example request:

```json
{
  "file_location": "../data_ingress/demo_data.csv",
  "control_file_location": "../data_ingress/control_csv.properties"
}
```

The supplied CSV demo has 101 physical rows. After invalid-email rejection and duplicate employee-ID rejection, 84 distinct rows load into a clean database. Use `record_count=84` for the first CSV load. If those employees already exist, duplicate protection rejects them and the rerun loads 0 new rows; the current `control_csv.properties` is set to `record_count=0` for that rerun scenario. For the JSON demo, use `control.properties` (`record_count=18`).

If the MySQL table was created before Base64 encoding was added, migrate the salary column once:

```sql
ALTER TABLE employees MODIFY salary VARCHAR(32) NOT NULL DEFAULT 'MA==';
```

For an existing database, inspect duplicates before adding the unique employee constraint:

```sql
SELECT employee_id, COUNT(*) AS occurrences
FROM employees
GROUP BY employee_id
HAVING COUNT(*) > 1;

ALTER TABLE employees ADD UNIQUE KEY uq_employees_employee_id (employee_id);
```

Example error record:

```json
{"execution_id":"...","timestamp":"2026-09-17T12:00:00Z","employee_id":"EMP_ERR","reason":"Invalid salary","raw_record":"EMP_ERR,..."}
```
