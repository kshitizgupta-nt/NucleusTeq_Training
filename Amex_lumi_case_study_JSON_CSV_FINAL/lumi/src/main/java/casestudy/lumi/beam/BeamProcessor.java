package casestudy.lumi.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BeamProcessor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_VALUE = " ";

    public static class EmployeeRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public String ingestionTimestamp;
        public String executionId;
        public String sourceCreationTime;
        public String employeeId;
        public String firstName;
        public String lastName;
        public String email;
        public String phoneNumber;
        public String hireDate;
        public String department;
        public String jobTitle;
        public String salary;
        public String currency;
        public String employmentStatus;
        public String managerId;
        public boolean isActive;
        public String skills;
        public String address;
        public String emergencyContact;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EmployeeRecord that = (EmployeeRecord) o;
            return java.util.Objects.equals(employeeId, that.employeeId) && java.util.Objects.equals(executionId, that.executionId);
        }

        @Override
        public int hashCode() { return java.util.Objects.hash(employeeId, executionId); }
    }

    public static void runPipeline(String inputFile, String executionId, String fileFormat, String sourceCreationTime,
                                   String dbUrl, String dbUsername, String dbPassword, String errorLogDirectory) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        final String fileFormatType = fileFormat == null || fileFormat.isBlank()
            ? deduceFileFormat(inputFile)
            : fileFormat.trim().toLowerCase();
        if (!java.util.Set.of("json", "csv").contains(fileFormatType)) {
            throw new IllegalArgumentException("Unsupported file format: " + fileFormatType);
        }
        System.out.println("[BEAM] Starting ingestion: input=" + inputFile + ", format=" + fileFormatType
            + ", execution_id=" + executionId + ", source_creation_time=" + sourceCreationTime);
        System.out.println("[BEAM] JDBC target: " + dbUrl + ", user=" + dbUsername + ", error_log=" + errorLogDirectory);
        Set<String> existingEmployeeIds = loadExistingEmployeeIds(dbUrl, dbUsername, dbPassword);
        System.out.println("[BEAM] Existing employee IDs loaded for duplicate detection: " + existingEmployeeIds.size());

        pipeline
                .apply("ReadTextFile", TextIO.read().from(inputFile))
                .apply("DynamicFormatParser", ParDo.of(new DoFn<String, EmployeeRecord>() {
                    private final Counter parsedRecords = Metrics.counter("lumi", "parsed_records");
                    private final Counter rejectedRecords = Metrics.counter("lumi", "rejected_records");
                    @ProcessElement
                    public void processElement(@Element String element, OutputReceiver<EmployeeRecord> out) {
                        try {
                            EmployeeRecord record = new EmployeeRecord();
                            record.ingestionTimestamp = Instant.now().toString();
                            record.executionId = executionId;
                            record.sourceCreationTime = sourceCreationTime != null ? sourceCreationTime : Instant.now().toString();

                            boolean parsedSuccessfully = false;
                            switch (fileFormatType) {
                                case "json": parsedSuccessfully = parseJsonRow(element, record); break;
                                case "csv": parsedSuccessfully = parseCsvRow(element, record); break;
                            }

                            if (parsedSuccessfully) {
                                if (existingEmployeeIds.contains(record.employeeId)) {
                                    rejectedRecords.inc();
                                    throw new IllegalArgumentException("Duplicate employee_id already exists in database");
                                }
                                parsedRecords.inc();
                                System.out.println("[BEAM] Accepted row: employee_id=" + record.employeeId
                                        + ", execution_id=" + record.executionId
                                        + ", ingestion_timestamp=" + record.ingestionTimestamp);
                                out.output(record);
                            } else {
                                rejectedRecords.inc();
                                throw new IllegalArgumentException(rejectionReason(element, fileFormatType));
                            }
                        } catch (Exception e) {
                            System.err.println("[BEAM] Rejected row for execution_id=" + executionId
                                    + ": " + e.getMessage() + "; row=" + abbreviate(element));
                                appendErrorRecord(errorLogDirectory, executionId, fileFormatType,
                                    inputFile, extractEmployeeId(element), e.getMessage(), element);
                        }
                    }
                }))
                .setCoder(org.apache.beam.sdk.coders.SerializableCoder.of(EmployeeRecord.class))
                .apply("KeyByEmployeeId", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                    TypeDescriptor.of(EmployeeRecord.class)))
                        .via(record -> KV.of(record.employeeId, record)))
                .apply("GroupDuplicateEmployeeIds", GroupByKey.create())
                .apply("RejectDuplicateInputRows", ParDo.of(new DoFn<KV<String, Iterable<EmployeeRecord>>, EmployeeRecord>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Iterable<EmployeeRecord>> grouped,
                                                OutputReceiver<EmployeeRecord> out) {
                        boolean first = true;
                        for (EmployeeRecord record : grouped.getValue()) {
                            if (first) {
                                first = false;
                                out.output(record);
                            } else {
                                appendErrorRecord(errorLogDirectory, executionId, fileFormatType, inputFile,
                                        record.employeeId,
                                        "Duplicate employee_id repeated in input file",
                                        "employee_id=" + record.employeeId);
                            }
                        }
                    }
                }))
                .apply("WriteToMySQL", JdbcIO.<EmployeeRecord>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", dbUrl).withUsername(dbUsername).withPassword(dbPassword))
                        .withStatement("INSERT INTO employees (ingestion_timestamp, execution_id, source_creation_time, employee_id, first_name, last_name, email, phone_number, hire_date, department, job_title, salary, currency, employment_status, manager_id, is_active, skills, address, emergency_contact) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        .withPreparedStatementSetter((EmployeeRecord record, PreparedStatement query) -> {
                            System.out.println("[BEAM] JDBC write: employee_id=" + record.employeeId
                                    + ", execution_id=" + record.executionId
                                    + ", source_creation_time=" + record.sourceCreationTime);
                            query.setString(1, record.ingestionTimestamp); query.setString(2, record.executionId); query.setString(3, record.sourceCreationTime);
                            query.setString(4, record.employeeId); query.setString(5, record.firstName); query.setString(6, record.lastName);
                            query.setString(7, record.email); query.setString(8, record.phoneNumber); query.setString(9, record.hireDate);
                            query.setString(10, record.department); query.setString(11, record.jobTitle); query.setString(12, record.salary);
                            query.setString(13, record.currency); query.setString(14, record.employmentStatus); query.setString(15, record.managerId);
                            query.setBoolean(16, record.isActive); query.setString(17, record.skills); query.setString(18, record.address); query.setString(19, record.emergencyContact);
                        })
                );
        pipeline.run().waitUntilFinish();
        System.out.println("[BEAM] Pipeline finished for execution_id=" + executionId
            + ". Check Airflow validation for the committed database count.");
    }

    public static String deduceFileFormat(String path) {
        String lower = path.toLowerCase();
        if (lower.endsWith(".json") || lower.endsWith(".jsonl")) return "json";
        if (lower.endsWith(".csv")) return "csv";
        throw new IllegalArgumentException("Only JSON/JSONL and CSV files are supported: " + path);
    }

    private static Set<String> loadExistingEmployeeIds(String dbUrl, String dbUsername, String dbPassword) {
        Set<String> employeeIds = new HashSet<>();
        try (var connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             var statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT employee_id FROM employees")) {
            while (resultSet.next()) employeeIds.add(resultSet.getString(1));
        } catch (SQLException exception) {
            throw new IllegalStateException("Unable to load existing employee IDs for duplicate detection", exception);
        }
        return employeeIds;
    }

    private static String abbreviate(String value) {
        if (value == null) return "<null>";
        String compact = value.replaceAll("\\s+", " ");
        return compact.length() <= 180 ? compact : compact.substring(0, 180) + "...";
    }

    private static String rejectionReason(String element, String format) {
        if (element == null || element.trim().isEmpty()) return "Empty record";
        if ("json".equals(format)) {
            String trimmed = element.trim();
            if (trimmed.startsWith("[")) return "JSON array found where one JSON object per line is required";
            try {
                JsonNode node = OBJECT_MAPPER.readTree(trimmed);
                if (node == null || !node.isObject()) return "JSON record is not an object";
                if (!node.has("employee_id") || node.get("employee_id").asText().isBlank()) return "Missing employee_id";
                if (node.has("salary") && !node.get("salary").asText().trim().matches("-?\\d+")) return "salary is not an integer";
                return "JSON field validation failed";
            } catch (Exception exception) {
                return "Malformed JSON: " + exception.getMessage();
            }
        }
        if ("csv".equals(format)) {
            if (element.toLowerCase().startsWith("employee_id,")) return "CSV header row";
            return "CSV field validation failed; inspect splitter validation for the exact field";
        }
        return "Unsupported format: " + format;
    }

    public static boolean parseJsonRow(String element, EmployeeRecord r) {
        try {
            String trimmed = element == null ? "" : element.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("[")) return false;
            JsonNode j = OBJECT_MAPPER.readTree(trimmed);
            if (j == null || !j.isObject()) return false;
            populateFromJson(j, r);
            return !r.employeeId.equals(DEFAULT_VALUE);
        } catch (Exception e) { return false; }
    }

    private static void populateFromJson(JsonNode j, EmployeeRecord r) {
        r.employeeId = textOrDefault(j, "employee_id");
        r.firstName = textOrDefault(j, "first_name");
        r.lastName = textOrDefault(j, "last_name");
        r.email = textOrDefault(j, "email");
        r.phoneNumber = textOrDefault(j, "phone_number");
        r.hireDate = textOrDefault(j, "hire_date");
        r.department = textOrDefault(j, "department");
        r.jobTitle = textOrDefault(j, "job_title");
        r.salary = encodeSalary(integerOrDefault(j, "salary"));
        r.currency = j.has("currency") ? textOrDefault(j, "currency") : "USD";
        r.employmentStatus = textOrDefault(j, "employment_status");
        r.managerId = textOrDefault(j, "manager_id");
        r.isActive = booleanOrDefault(j, "is_active", true);
        r.skills = j.has("skills") ? j.get("skills").toString() : "[]";
        r.address = j.has("address") ? j.get("address").toString() : "{}";
        r.emergencyContact = encodeEmergencyContact(j.get("emergency_contact"));
        r.phoneNumber = encodeSensitiveValue(r.phoneNumber);

        if (r.employeeId.length() > 100 || r.firstName.length() > 255 || r.lastName.length() > 255
            || r.email.length() > 255 || r.phoneNumber.length() > 100 || r.hireDate.length() > 50
            || r.department.length() > 255 || r.jobTitle.length() > 255 || r.currency.length() > 20
            || r.employmentStatus.length() > 100 || r.managerId.length() > 100) {
            r.employeeId = DEFAULT_VALUE;
        }
    }

    public static boolean parseCsvFields(String element, EmployeeRecord record) {
        return parseCsvRow(element, record);
    }
    // --- TRACKS COLUMN ORDER DYNAMICALLY AND DROPS HEADER RECORDS ---
    private static java.util.Map<String, Integer> csvHeaderMap = null;

    public static boolean parseCsvRow(String element, EmployeeRecord record) {
        try {
            String trimmedLine = element == null ? "" : element.trim();
            if (trimmedLine.isEmpty()) return false;

            // Tokenize CSV row respecting encapsulated quotes safely
            List<String> tokens = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < trimmedLine.length(); i++) {
                char c = trimmedLine.charAt(i);
                if (c == '"') {
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    tokens.add(sb.toString().trim());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
            tokens.add(sb.toString().trim());

            // 1. DYNAMIC HEADER TRACKING & EXTRACTION FILTER
            if (tokens.get(0).toLowerCase().contains("employee_id") || tokens.get(0).toLowerCase().contains("emplo")) {
                csvHeaderMap = new java.util.HashMap<>();
                for (int i = 0; i < tokens.size(); i++) {
                    csvHeaderMap.put(tokens.get(i).toLowerCase().trim(), i);
                }
                return false; // Drops the header record instantly from database ingestion loops!
            }

            // Fallback to sequential map layout if no formal file handshake occurred
            if (csvHeaderMap == null || csvHeaderMap.isEmpty()) {
                csvHeaderMap = new java.util.HashMap<>();
                csvHeaderMap.put("employee_id", 0);
                csvHeaderMap.put("first_name", 1);
                csvHeaderMap.put("last_name", 2);
                csvHeaderMap.put("email", 3);
                csvHeaderMap.put("phone_number", 4);
                csvHeaderMap.put("hire_date", 5);
                csvHeaderMap.put("department", 6);
                csvHeaderMap.put("job_title", 7);
                csvHeaderMap.put("salary", 8);
                csvHeaderMap.put("currency", 9);
                csvHeaderMap.put("employment_status", 10);
                csvHeaderMap.put("manager_id", 11);
                csvHeaderMap.put("is_active", 12);
                csvHeaderMap.put("skills", 13);
                csvHeaderMap.put("address_street", 14);
                csvHeaderMap.put("address_city", 15);
                csvHeaderMap.put("address_state", 16);
                csvHeaderMap.put("address_postal_code", 17);
                csvHeaderMap.put("address_country", 18);
                csvHeaderMap.put("emergency_contact_name", 19);
                csvHeaderMap.put("emergency_contact_relationship", 20);
                csvHeaderMap.put("emergency_contact_phone", 21);
                csvHeaderMap.put("emergency_contact_email", 22);
            }

            // 2. DYNAMIC FIELD EXTRACTION BASED ON RESOLVED HEADERS
            record.employeeId = getCsvValue(tokens, "employee_id");
            record.firstName = getCsvValue(tokens, "first_name");
            record.lastName = getCsvValue(tokens, "last_name");
            record.email = getCsvValue(tokens, "email");
            record.phoneNumber = encodeSensitiveValue(getCsvValue(tokens, "phone_number"));
            record.hireDate = getCsvValue(tokens, "hire_date");
            record.department = getCsvValue(tokens, "department");
            record.jobTitle = getCsvValue(tokens, "job_title");

            // Extract and parse salary metrics cleanly
            String salaryValue = getCsvValue(tokens, "salary").trim();
            record.salary = encodeSalary(salaryValue.isEmpty() ? 0 : Integer.parseInt(salaryValue));

            record.currency = getCsvValue(tokens, "currency").equals(DEFAULT_VALUE) ? "USD" : getCsvValue(tokens, "currency");
            record.employmentStatus = getCsvValue(tokens, "employment_status");
            record.managerId = getCsvValue(tokens, "manager_id");

            String activeStr = getCsvValue(tokens, "is_active").toLowerCase();
            record.isActive = activeStr.contains("true") || activeStr.contains("1") || activeStr.equals(DEFAULT_VALUE);

            // 3. SECURE NESTED STRUCTURES SETUP
            record.skills = csvSkillsJson(getCsvValue(tokens, "skills"));
            record.address = csvAddressJson(tokens);

            // Handle emergency contact text layout capture dynamically
            String emergencyStr = getCsvValue(tokens, "emergency_contact");
            record.emergencyContact = csvEmergencyContactJson(tokens, emergencyStr);

            // Enforce table character length constraints bounds validation
                if (record.employeeId.length() > 100 || record.firstName.length() > 255 || record.lastName.length() > 255
                    || record.email.length() > 255 || record.phoneNumber.length() > 100 || record.hireDate.length() > 50
                    || record.department.length() > 255 || record.jobTitle.length() > 255 || record.currency.length() > 20
                    || record.employmentStatus.length() > 100 || record.managerId.length() > 100) {
                return false;
            }

            return !record.employeeId.equals(DEFAULT_VALUE);
        } catch (Exception e) {
            return false;
        }
    }

    private static String getCsvValue(List<String> tokens, String columnName) {
        if (csvHeaderMap != null && csvHeaderMap.containsKey(columnName)) {
            int idx = csvHeaderMap.get(columnName);
            if (idx < tokens.size()) {
                String val = tokens.get(idx).trim();
                return val.isEmpty() ? DEFAULT_VALUE : val;
            }
        }
        return DEFAULT_VALUE;
    }

    private static String csvSkillsJson(String value) {
        if (value == null || value.isBlank() || DEFAULT_VALUE.equals(value)) return "[]";
        try {
            com.fasterxml.jackson.databind.node.ArrayNode skills = OBJECT_MAPPER.createArrayNode();
            for (String skill : value.split(",")) if (!skill.trim().isEmpty()) skills.add(skill.trim());
            return skills.toString();
        } catch (Exception ignored) { return "[]"; }
    }

    private static String csvAddressJson(List<String> tokens) {
        com.fasterxml.jackson.databind.node.ObjectNode address = OBJECT_MAPPER.createObjectNode();
        address.put("street", getCsvValue(tokens, "address_street"));
        address.put("city", getCsvValue(tokens, "address_city"));
        address.put("state", getCsvValue(tokens, "address_state"));
        address.put("postal_code", getCsvValue(tokens, "address_postal_code"));
        address.put("country", getCsvValue(tokens, "address_country"));
        return address.toString();
    }

    private static String csvEmergencyContactJson(List<String> tokens, String fallback) {
        com.fasterxml.jackson.databind.node.ObjectNode contact = OBJECT_MAPPER.createObjectNode();
        contact.put("name", getCsvValue(tokens, "emergency_contact_name"));
        contact.put("relationship", getCsvValue(tokens, "emergency_contact_relationship"));
        contact.put("phone", encodeSensitiveValue(getCsvValue(tokens, "emergency_contact_phone")));
        contact.put("email", getCsvValue(tokens, "emergency_contact_email"));
        if (contact.get("name").asText().equals(DEFAULT_VALUE) && fallback != null && !fallback.equals(DEFAULT_VALUE)) {
            return encodeEmergencyContactText(fallback);
        }
        return contact.toString();
    }


    public static String textOrDefault(JsonNode n, String f) {
        JsonNode v = n.get(f);
        return v == null || v.isNull() || v.asText().trim().isEmpty() ? DEFAULT_VALUE : v.asText().trim();
    }

    public static String encodeSalary(int value) {
        return encodeSensitiveValue(Integer.toString(value));
    }

    public static String encodeSensitiveValue(String value) {
        if (value == null || value.trim().isEmpty() || DEFAULT_VALUE.equals(value)) return DEFAULT_VALUE;
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String encodeEmergencyContact(JsonNode value) {
        if (value == null || value.isNull() || !value.isObject()) return "{}";
        JsonNode copy = value.deepCopy();
        JsonNode phone = copy.get("phone");
        if (phone != null && !phone.isNull()) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) copy).put("phone", encodeSensitiveValue(phone.asText()));
        }
        return copy.toString();
    }

    public static String encodeEmergencyContactText(String value) {
        if (value == null || value.trim().isEmpty() || DEFAULT_VALUE.equals(value)) return "{}";
        try { return encodeEmergencyContact(OBJECT_MAPPER.readTree(value)); }
        catch (Exception ignored) { return value; }
    }

    public static int integerOrDefault(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) return 0;
        if (!v.isNumber() && !v.asText().trim().matches("-?\\d+")) throw new IllegalArgumentException(field + " must be numeric");
        return v.asInt();
    }

    public static boolean booleanOrDefault(JsonNode n, String field, boolean defaultValue) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) return defaultValue;
        if (v.isBoolean()) return v.asBoolean();
        if ("true".equalsIgnoreCase(v.asText().trim())) return true;
        if ("false".equalsIgnoreCase(v.asText().trim())) return false;
        throw new IllegalArgumentException(field + " must be true or false");
    }

    public static String valueOrDefault(String v) { return v == null || v.trim().isEmpty() ? DEFAULT_VALUE : v.trim(); }

    public static synchronized void appendErrorRecord(String dir, String executionId, String format,
                                                      String sourceFile, String employeeId, String reason,
                                                      String element) {
        try {
            java.nio.file.Path d = java.nio.file.Paths.get(dir);
            java.nio.file.Files.createDirectories(d);
            try (java.io.FileWriter w = new java.io.FileWriter(d.resolve("error_records.txt").toFile(), true)) {
                String safeReason = reason == null || reason.isBlank() ? "Unknown ingestion error" : reason;
                String record = "{\"execution_id\":\"" + escapeJson(executionId)
                        + "\",\"timestamp\":\"" + Instant.now()
                        + "\",\"stage\":\"beam_parser\",\"format\":\"" + escapeJson(format)
                        + "\",\"source_file\":\"" + escapeJson(sourceFile)
                        + "\",\"employee_id\":\"" + escapeJson(employeeId)
                        + "\",\"reason\":\"" + escapeJson(safeReason)
                        + "\",\"raw_record\":\"" + escapeJson(element) + "\"}";
                w.write(record); w.write(System.lineSeparator());
            }
        } catch (Exception e) { System.err.println("Unable to write error record: " + e.getMessage()); }
    }

    private static String extractEmployeeId(String element) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(element);
            if (node != null && node.isObject() && node.has("employee_id")) return node.get("employee_id").asText(DEFAULT_VALUE);
        } catch (Exception ignored) { }
        if (element != null && !element.trim().isEmpty() && !element.contains(",")) return element.trim();
        return DEFAULT_VALUE;
    }

    private static String escapeJson(String value) {
        if (value == null) return " ";
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\r", "\\r").replace("\n", "\\n");
    }

    public static void setDefaults(EmployeeRecord r, String executionId, String sourceCreationTime) {
        r.executionId = executionId != null ? executionId : DEFAULT_VALUE;
        r.sourceCreationTime = sourceCreationTime != null ? sourceCreationTime : Instant.now().toString();
        r.ingestionTimestamp = Instant.now().toString();
    }

    public static List<EmployeeRecord> parseJsonContent(String content, String executionId, String sourceCreationTime) throws Exception {
        List<EmployeeRecord> result = new ArrayList<>();
        String trimmed = content == null ? "" : content.trim();
        if (trimmed.isEmpty()) return result;

        if (trimmed.startsWith("[")) {
            JsonNode root = OBJECT_MAPPER.readTree(trimmed);
            if (root.isArray()) {
                for (JsonNode node : root) {
                    EmployeeRecord r = new EmployeeRecord();
                    setDefaults(r, executionId, sourceCreationTime);
                    if (node.isObject()) {
                        populateFromJson(node, r);
                        if (!r.employeeId.equals(DEFAULT_VALUE)) result.add(r);
                    }
                }
            }
            return result;
        }

        for (String line : content.split("\\R")) {
            if (line.trim().isEmpty()) continue;
            EmployeeRecord r = new EmployeeRecord();
            setDefaults(r, executionId, sourceCreationTime);
            try {
                JsonNode j = OBJECT_MAPPER.readTree(line);
                if (j != null && j.isObject()) {
                    populateFromJson(j, r);
                    if (!r.employeeId.equals(DEFAULT_VALUE)) result.add(r);
                }
            } catch (Exception ignored) {}
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(">>> Initializing Apache Beam JVM Entry Point via Airflow Handshake <<<");
        if (args.length != 8) {
            System.err.println("Usage: java BeamProcessor <inputFile> <executionId> <fileFormat> <sourceCreationTime> <dbUrl> <dbUsername> <dbPassword> <errorLogDir>");
            System.exit(1);
        }
        try {
            runPipeline(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
            System.exit(0);
        } catch (Exception e) {
            System.err.println("CRITICAL: Apache Beam execution crashed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
