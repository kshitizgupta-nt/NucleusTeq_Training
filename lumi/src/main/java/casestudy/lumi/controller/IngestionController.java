package casestudy.lumi.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/v1/ingestion")
public class IngestionController {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionController.class);

    private final RestTemplate restTemplate;

    @Value("${amex.ingestion.airflow-url:http://localhost:8081/api/v1/dags/amex_employee_ingestion/dagRuns}")
    private String airflowUrl;

    @Value("${amex.ingestion.airflow-username:airflow}")
    private String airflowUsername;

    @Value("${amex.ingestion.airflow-password:airflow}")
    private String airflowPassword;

    @Value("${amex.ingestion.file-size-threshold-bytes:200}")
    private long fileSizeThreshold;

    @Value("${amex.ingestion.split-chunks:3}")
    private int splitChunks;

    @Value("${amex.ingestion.python-executable:python}")
    private String pythonExecutable;

    @Value("${amex.ingestion.splitter-script:../data_ingress/file_splitter.py}")
    private String splitterScript;

    @Value("${amex.ingestion.split-output-directory:../data_ingress/split_chunks}")
    private String splitOutputDirectory;

    @Value("${amex.ingestion.airflow-data-root:/opt/airflow/data_ingress}")
    private String airflowDataRoot;

    @Value("${amex.ingestion.host-data-root:../data_ingress}")
    private String hostDataRoot;

    public IngestionController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @PostMapping(value="/trigger", consumes=MediaType.APPLICATION_JSON_VALUE, produces=MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String,Object>> triggerIngestion(@RequestBody Map<String,String> requestPayload) {
        Map<String, Object> response = new HashMap<>();

        if (requestPayload == null) {
            return error(HttpStatus.BAD_REQUEST, "Request body is required.");
        }

        String fileLocation = requestPayload.get("file_location");
        String controlFileLocation = requestPayload.get("control_file_location");
        LOGGER.info("Ingestion request received: file_location={}, control_file_location={}", fileLocation, controlFileLocation);

        if (fileLocation == null || fileLocation.isEmpty()) return error(HttpStatus.BAD_REQUEST, "'file_location' is required.");
        if (controlFileLocation == null || controlFileLocation.isEmpty()) return error(HttpStatus.BAD_REQUEST, "'control_file_location' is required.");

        Path targetFile = resolveHostPath(fileLocation);
        Path targetControl = resolveHostPath(controlFileLocation);
        File dataFile = targetFile.toFile();
        File controlFile = targetControl.toFile();
        LOGGER.info("Resolved host paths: data={}, control={}, data_exists={}, control_exists={}",
            targetFile, targetControl, dataFile.isFile(), controlFile.isFile());

        if (!dataFile.exists()) {
            return error(HttpStatus.BAD_REQUEST, "Source data file not found: " + targetFile);
        }
        if (!controlFile.exists()) {
            return error(HttpStatus.BAD_REQUEST, "Control file not found: " + targetControl);
        }

        // Read Expected Record Count
        int expectedRecordCount = -1;
        try {
            expectedRecordCount = readExpectedRecordCount(targetControl);
            if (expectedRecordCount < 0) {
                throw new IllegalArgumentException("record_count must not be negative");
            }
        } catch (Exception e) {
            return error(HttpStatus.BAD_REQUEST, "Invalid control file: " + e.getMessage());
        }

        long fileSizeBytes = dataFile.length();
        String sourceCreationTime = java.time.Instant.now().toString();
        try {
            sourceCreationTime = Files.readAttributes(targetFile, BasicFileAttributes.class).creationTime().toInstant().toString();
        } catch (IOException ignored) {}

        String executionId = UUID.randomUUID().toString();
        String fileFormat = detectFileFormat(targetFile.toString());
        boolean splitApplied = shouldSplit(fileSizeBytes);
        Path airflowInputPath = targetFile;
        LOGGER.info("Prepared ingestion: execution_id={}, format={}, size_bytes={}, expected_count={}, split={}",
            executionId, fileFormat, fileSizeBytes, expectedRecordCount, splitApplied);

        if (splitApplied) {
            try {
                runSplitter(targetFile, executionId);
                airflowInputPath = Paths.get(splitOutputDirectory).toAbsolutePath().normalize();
                LOGGER.info("Splitter completed: output_directory={}, files={}", airflowInputPath,
                        Files.list(airflowInputPath).map(Path::getFileName).toList());
            } catch (Exception e) {
                LOGGER.error("Splitter failed for execution_id={}", executionId, e);
                return error(HttpStatus.INTERNAL_SERVER_ERROR, "File splitting failed: " + e.getMessage());
            }
        }

        String finalAirflowFileLocation = toAirflowPath(airflowInputPath, splitApplied);
        String finalAirflowControlLocation = airflowDataRoot + "/" + targetControl.getFileName();

        // Call Containerized Airflow Endpoint Engine
        try {
            Map<String, Object> conf = new HashMap<>();
            conf.put("file_location", finalAirflowFileLocation);
            conf.put("control_file_location", finalAirflowControlLocation);
            conf.put("execution_id", executionId);
            conf.put("expected_count", expectedRecordCount);
            conf.put("file_format", fileFormat);
            conf.put("source_creation_time", sourceCreationTime);
                LOGGER.info("Triggering Airflow: execution_id={}, airflow_file={}, control_file={}, format={}, expected_count={}",
                    executionId, finalAirflowFileLocation, finalAirflowControlLocation, fileFormat, expectedRecordCount);

            Map<String, Object> body = new HashMap<>();
            body.put("conf", conf);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(airflowUsername, airflowPassword);

            var airflowResponse = restTemplate.postForEntity(airflowUrl, new org.springframework.http.HttpEntity<>(body, headers), String.class);
            LOGGER.info("Airflow accepted execution_id={}, status={}", executionId, airflowResponse.getStatusCode());
        } catch (Exception e) {
            LOGGER.error("Airflow trigger failed for execution_id={}", executionId, e);
            return error(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to handshake with Airflow: " + e.getMessage());
        }

        response.put("status", "SUCCESS");
        response.put("execution_id", executionId);
        response.put("file_size_bytes", fileSizeBytes);
        response.put("split_applied", splitApplied);
        response.put("expected_record_count", expectedRecordCount);
        response.put("file_location", finalAirflowFileLocation);

        return ResponseEntity.ok(response);
    }

    public int readExpectedRecordCount(Path controlPath) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(controlPath.toFile())) {
            properties.load(input);
        }
        String value = properties.getProperty("record_count");
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("record_count is missing");
        int recordCount = Integer.parseInt(value.trim());
        if (recordCount < 0) throw new IllegalArgumentException("record_count must not be negative");
        return recordCount;
    }

    public boolean shouldSplit(long fileSizeBytes) { return fileSizeBytes > fileSizeThreshold; }

    private void runSplitter(Path source, String executionId) throws Exception {
        Process process = new ProcessBuilder(
                pythonExecutable,
                Paths.get(splitterScript).toAbsolutePath().normalize().toString(),
                source.toString(),
                Paths.get(splitOutputDirectory).toAbsolutePath().normalize().toString(),
                Integer.toString(splitChunks),
                executionId)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (!process.waitFor(10, TimeUnit.MINUTES) || process.exitValue() != 0) {
            throw new IllegalStateException(output.trim());
        }
        LOGGER.info("Splitter process output: {}", output.trim().replaceAll("\\R+", " | "));
    }

    private String toAirflowPath(Path path, boolean split) {
        String name = path.getFileName().toString();
        return airflowDataRoot + "/" + name + (split ? "/part-*" : "");
    }

    private Path resolveHostPath(String requestedPath) {
        String normalized = requestedPath.replace('\\', '/');
        String airflowPrefix = airflowDataRoot.replace('\\', '/').replaceAll("/$", "");
        if (normalized.equals(airflowPrefix) || normalized.startsWith(airflowPrefix + "/")) {
            String relativePath = normalized.substring(airflowPrefix.length()).replaceFirst("^/", "");
            return Paths.get(hostDataRoot).resolve(relativePath).toAbsolutePath().normalize();
        }
        return Paths.get(requestedPath).toAbsolutePath().normalize();
    }

    public String detectFileFormat(String path) {
        String lower = path.toLowerCase();
        if (lower.endsWith(".csv")) return "csv";
        if (lower.endsWith(".json") || lower.endsWith(".jsonl")) return "json";
        throw new IllegalArgumentException("Only JSON/JSONL and CSV files are supported: " + path);
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "FAILED");
        response.put("message", message);
        return ResponseEntity.status(status).body(response);
    }
}
