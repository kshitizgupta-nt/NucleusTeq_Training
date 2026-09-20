import csv
import io
import json
import os
import re
import shutil
import sys
import traceback
import uuid
from pathlib import Path

from pyspark.sql import SparkSession


def _error_record(execution_id: str, employee_id: str, reason: str, raw_record: str,
                  stage: str, source_file: str, line_number: int | None = None) -> str:
    return json.dumps({
        "execution_id": execution_id,
        "timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "stage": stage,
        "source_file": source_file,
        "line_number": line_number,
        "employee_id": employee_id or " ",
        "reason": reason,
        "raw_record": raw_record,
    }, ensure_ascii=False)


def _write_errors(path: Path, errors: list[str]) -> None:
    if errors:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(errors) + "\n")


def _json_records(source: Path, execution_id: str):
    """Return valid JSON employee records and malformed JSON rows."""
    text = source.read_text(encoding="utf-8")
    stripped = text.strip()

    if not stripped:
        return [], []

    try:
        root = json.loads(stripped)

        if isinstance(root, list):
            records = [
                json.dumps(item, separators=(",", ":"), ensure_ascii=False)
                for item in root
                if isinstance(item, dict)
            ]
            return records, []

        if isinstance(root, dict):
            return [json.dumps(root, separators=(",", ":"), ensure_ascii=False)], []

    except json.JSONDecodeError:
        pass

    records = []
    errors = []

    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue

        try:
            value = json.loads(line)

            if not isinstance(value, dict):
                raise ValueError("JSON record must be an object")

            records.append(json.dumps(value, separators=(",", ":"), ensure_ascii=False))

        except Exception as exc:
            errors.append(_error_record(execution_id, " ", f"JSON parse failed: {exc}", line,
                                        "splitter", str(source), line_number))

    return records, errors


def _write_local_partitions(records, output: Path, number_of_chunks: int) -> None:
    output.mkdir(parents=True, exist_ok=True)
    partitions = [[] for _ in range(number_of_chunks)]
    for index, record in enumerate(records):
        partitions[index % number_of_chunks].append(record)

    for index, partition in enumerate(partitions):
        part_file = output / f"part-{index:05d}"
        part_file.write_text("\n".join(partition) + ("\n" if partition else ""), encoding="utf-8")

    (output / "_SUCCESS").touch()
    print(f"[SPLITTER] Wrote {len(records)} records to {output} in {number_of_chunks} partitions")


def _split_on_windows(source: Path, output: Path, number_of_chunks: int, execution_id: str) -> None:
    if source.suffix.lower() in {".json", ".jsonl"}:
        records, errors = _json_records(source, execution_id)
        print(f"[SPLITTER] JSON input={source}; valid_records={len(records)}; rejected_records={len(errors)}")
        if errors:
            _write_errors(output.parent / "error_logs" / "error_records.txt", errors)
        if not records:
            raise ValueError("Source JSON contains no valid employee records")
        _write_local_partitions(records, output, number_of_chunks)
        return

    if source.suffix.lower() == ".csv":
        records = []
        errors = []
        canonical_columns = [
            "employee_id", "first_name", "last_name", "email",
            "phone_number", "hire_date", "department", "job_title", "salary",
            "currency", "employment_status", "manager_id", "is_active", "skills",
            "address_street", "address_city", "address_state", "address_postal_code",
            "address_country", "emergency_contact_name", "emergency_contact_relationship",
            "emergency_contact_phone", "emergency_contact_email",
        ]
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = [column.strip().lower() for column in next(reader)]
            required_columns = {
                "employee_id", "first_name", "last_name", "email",
                "salary", "phone_number", "department", "job_title",
            }
            missing_columns = required_columns - set(header)
            if missing_columns:
                raise ValueError("CSV is missing required columns: " + ", ".join(sorted(missing_columns)))

            for line_number, row in enumerate(reader, start=2):
                if not row or not any(field.strip() for field in row):
                    continue
                if len(row) != len(header):
                    errors.append(_error_record(execution_id, row[0] if row else " ", "Invalid column count", ",".join(row),
                                                "splitter", str(source), line_number))
                    continue
                row = [field.strip() for field in row]
                if not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", row[header.index("email")]):
                    errors.append(_error_record(execution_id, row[header.index("employee_id")], "Invalid email", ",".join(row),
                                                "splitter", str(source), line_number))
                    continue
                if not re.fullmatch(r"-?\d+", row[header.index("salary")]):
                    errors.append(_error_record(execution_id, row[header.index("employee_id")], "Invalid salary", ",".join(row),
                                                "splitter", str(source), line_number))
                    continue
                buffer = io.StringIO()
                canonical_row = [row[header.index(column)] if column in header else "" for column in canonical_columns]
                csv.writer(buffer, lineterminator="").writerow(canonical_row)
                records.append(buffer.getvalue())

        if errors:
            _write_errors(output.parent / "error_logs" / "error_records.txt", errors)
            print(f"[SPLITTER] CSV input={source}; valid_records={len(records)}; rejected_records={len(errors)}")
        if not records:
            raise ValueError("Source CSV contains no valid employee records")
        _write_local_partitions(records, output, number_of_chunks)
        return

    raise ValueError("Only JSON/JSONL and CSV files are supported")


def split_file(source_file_path: str, output_directory: str, number_of_chunks: int, execution_id: str = "splitter") -> None:
    if number_of_chunks <= 0:
        raise ValueError("number_of_chunks must be greater than zero")

    source = Path(source_file_path).resolve()
    output = Path(output_directory).resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Source file not found: {source}")

    if output.exists():
        shutil.rmtree(output)

    output.parent.mkdir(parents=True, exist_ok=True)

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        local_hadoop_home = Path.home() / "hadoop"
        if (local_hadoop_home / "bin" / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = str(local_hadoop_home)
            os.environ["hadoop.home.dir"] = str(local_hadoop_home)

    if os.name == "nt":
        _split_on_windows(source, output, number_of_chunks, execution_id)
        print(f"[Local] Split complete: {source} -> {output} ({number_of_chunks} partitions)")
        return

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = (
        SparkSession.builder
        .appName("AmexLumiFileSplitter")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.worker.connect.timeout", "60s")
        .config("spark.sql.warehouse.dir", str(output.parent / "spark-warehouse"))
        .getOrCreate()
    )

    try:
        # ==========================================================
        # JSON / JSONL
        # ==========================================================
        if source.suffix.lower() in {".json", ".jsonl"}:
            records, errors = _json_records(source, execution_id)

            if errors:
                error_file = output.parent / "error_logs" / "error_records.txt"
                _write_errors(error_file, errors)

                print(
                    f"[PySpark] Rejected {len(errors)} "
                    f"malformed JSON record(s); "
                    f"details written to {error_file}",
                    file=sys.stderr
                )

            if not records:
                raise ValueError("Source JSON contains no valid employee records")

            rdd = spark.sparkContext.parallelize(records, number_of_chunks)

        # CSV
        elif source.suffix.lower() == ".csv":
            records = []
            errors = []

            with source.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)

                header = next(reader)

                header = [column.strip().lower() for column in header]

                print(f"[PySpark] CSV header: {header}")

                required_columns = {
                    "employee_id",
                    "first_name",
                    "last_name",
                    "email",
                    "salary",
                    "phone_number",
                    "department",
                    "job_title"
                }

                missing_columns = required_columns - set(header)

                if missing_columns:
                    raise ValueError(
                        "CSV is missing required columns: "
                        + ", ".join(sorted(missing_columns))
                    )

                email_index = header.index("email")
                salary_index = header.index("salary")

                for line_number, row in enumerate(reader, start=2):
                    if not row or not any(field.strip() for field in row):
                        continue

                    print(f"[PySpark] Line {line_number}: {row}")

                    if len(row) != len(header):
                        error = (
                            f"line {line_number}: "
                            f"{','.join(row)} | "
                            f"Invalid column count "
                            f"(expected {len(header)}, "
                            f"found {len(row)})"
                        )

                        print(f"[PySpark] REJECTED: {error}")

                        errors.append(_error_record(execution_id, row[0] if row else " ", error, ",".join(row), "splitter", str(source), line_number))
                        continue

                    row = [field.strip() for field in row]

                    email = row[email_index]

                    if not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                        error = f"line {line_number}: {','.join(row)} | Invalid email"

                        print(f"[PySpark] REJECTED: {error}")

                        errors.append(_error_record(execution_id, row[header.index("employee_id")], "Invalid email", ",".join(row), "splitter", str(source), line_number))
                        continue

                    salary = row[salary_index]

                    if not re.fullmatch(r"-?\d+", salary):
                        error = f"line {line_number}: {','.join(row)} | Invalid salary"

                        print(f"[PySpark] REJECTED: {error}")

                        errors.append(_error_record(execution_id, row[header.index("employee_id")], "Invalid salary", ",".join(row), "splitter", str(source), line_number))
                        continue

                    print(
                        f"[PySpark] ACCEPTED line {line_number}: "
                        f"employee_id={row[header.index('employee_id')]}"
                    )

                    canonical_columns = [
                        "employee_id", "first_name", "last_name", "email",
                        "phone_number", "hire_date", "department", "job_title", "salary",
                        "currency", "employment_status", "manager_id", "is_active", "skills",
                        "address_street", "address_city", "address_state", "address_postal_code",
                        "address_country", "emergency_contact_name", "emergency_contact_relationship",
                        "emergency_contact_phone", "emergency_contact_email",
                    ]
                    canonical_row = [
                        row[header.index(column)] if column in header else ""
                        for column in canonical_columns
                    ]
                    buffer = io.StringIO()
                    csv.writer(buffer, lineterminator="").writerow(canonical_row)
                    records.append(buffer.getvalue())

            if errors:
                error_file = output.parent / "error_logs" / "error_records.txt"
                _write_errors(error_file, errors)

                print(f"[PySpark] Rejected {len(errors)} invalid CSV record(s)")

            print(f"[PySpark] Valid CSV records: {len(records)}")

            if not records:
                raise ValueError("Source CSV contains no valid employee records")

            rdd = spark.sparkContext.parallelize(records, number_of_chunks)
        else:
            raise ValueError("Only JSON/JSONL and CSV files are supported")

        # ==========================================================
        # REPARTITION AND WRITE
        # ==========================================================
        rdd = rdd.repartition(number_of_chunks)

        staging_output = output.parent / f".{output.name}.staging-{uuid.uuid4().hex}"
        if staging_output.exists():
            shutil.rmtree(staging_output)
        rdd.saveAsTextFile(str(staging_output))
        if output.exists():
            shutil.rmtree(output)
        staging_output.replace(output)

        print(
            f"[PySpark] Split complete: "
            f"{source} -> {output} "
            f"({number_of_chunks} partitions)"
        )

    finally:
        spark.stop()


def main() -> None:
    if len(sys.argv) not in {4, 5}:
        print(
            "Usage: python file_splitter.py "
            "<source_file> "
            "<output_directory> "
            "<number_of_chunks> [execution_id]"
        )
        sys.exit(2)

    try:
        split_file(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4] if len(sys.argv) == 5 else "splitter")

    except Exception as exc:
        traceback.print_exc()
        print(f"[PySpark] Split failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

















