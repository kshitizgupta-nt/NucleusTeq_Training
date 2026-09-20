from pathlib import Path

ROOT = Path(__file__).parent
required = [
    "lumi/pom.xml",
    "lumi/mvnw.cmd",
    "lumi/src/main/java/casestudy/lumi/controller/IngestionController.java",
    "lumi/src/main/java/casestudy/lumi/beam/BeamProcessor.java",
    "lumi/src/main/resources/application.properties",
    "lumi/src/test/java/casestudy/lumi/beam/BeamProcessorTest.java",
    "lumi/src/test/java/casestudy/lumi/controller/IngestionControllerTest.java",
    "data_ingress/demo_data.jsonl",
    "data_ingress/demo_data.csv",
    "data_ingress/control.properties",
    "data_ingress/control_csv.properties",
    "data_ingress/file_splitter.py",
    "data_ingress/schema.sql",
    "airflow_workspace/dags/amex_ingestion_dag.py",
    "airflow_workspace/docker-compose.yaml",
    "airflow_workspace/Dockerfile",
]
missing = [p for p in required if not (ROOT / p).is_file()]
assert not missing, f"Missing required files: {missing}"
java = (ROOT / required[3]).read_text()
assert "parseJsonContent" in java and "parseCsvFields" in java
assert "sourceCreationTime" in java and "executionId" in java
assert "expected_count" in (ROOT / "airflow_workspace/dags/amex_ingestion_dag.py").read_text()
print("Project structure/content validation: PASS")
