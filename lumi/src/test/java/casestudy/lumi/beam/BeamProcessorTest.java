package casestudy.lumi.beam;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BeamProcessorTest {

    @Test
    void jsonObjectIsParsedAndMissingFieldsGetWhitespaceDefaults() throws Exception {
        BeamProcessor.EmployeeRecord r = new BeamProcessor.EmployeeRecord();
        BeamProcessor.setDefaults(r, "execution-1", "2026-01-01T00:00:00Z");
        assertTrue(BeamProcessor.parseJsonRow("{\"employee_id\":\"EMP0001\",\"first_name\":\"Arjun\",\"salary\":950000}", r));
        assertEquals("EMP0001", r.employeeId);
        assertEquals("Arjun", r.firstName);
        assertEquals(" ", r.lastName);
        assertEquals("OTUwMDAw", r.salary);
        assertEquals("execution-1", r.executionId);
        assertEquals("2026-01-01T00:00:00Z", r.sourceCreationTime);
        assertNotNull(r.ingestionTimestamp);
    }

      @Test
      void sensitivePhoneValuesAreBase64Encoded() {
        BeamProcessor.EmployeeRecord record = new BeamProcessor.EmployeeRecord();
        BeamProcessor.setDefaults(record, "execution-1", "source");
        assertTrue(BeamProcessor.parseJsonRow(
            "{\"employee_id\":\"EMP1\",\"phone_number\":\"+91-98765-43210\","
                + "\"salary\":950000,\"emergency_contact\":{\"phone\":\"+91-98765-43211\"}}", record));
        assertEquals("KzkxLTk4NzY1LTQzMjEw", record.phoneNumber);
        assertEquals("OTUwMDAw", record.salary);
        assertTrue(record.emergencyContact.contains("KzkxLTk4NzY1LTQzMjEx"));
      }

    @Test
    void jsonArrayIsParsedCorrectly() throws Exception {
        // FIXED: Added newline immediately after the opening delimiter
        String json = """
        [
          {"employee_id":"EMP1","first_name":"Arjun"},
          {"employee_id":"EMP2","first_name":"Priya"}
        ]""";
        List<BeamProcessor.EmployeeRecord> records = BeamProcessor.parseJsonContent(json, "execution-1", "source");
        assertEquals(2, records.size());
        assertEquals("EMP1", records.get(0).employeeId);
        assertEquals("EMP2", records.get(1).employeeId);
    }

      @Test
      void jsonUuidIdentifiersFitTheDatabaseSchema() {
        BeamProcessor.EmployeeRecord record = new BeamProcessor.EmployeeRecord();
        BeamProcessor.setDefaults(record, "execution-1", "source");
        assertTrue(BeamProcessor.parseJsonRow(
            "{\"employee_id\":\"9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d\","
                + "\"first_name\":\"Arjun\",\"manager_id\":\"1a2bc3de-4f5g-6h7i-8j9k-0l1m2n3o4p5q\","
                + "\"email\":\"arjun.sharma@techcorp.com\",\"salary\":950000}", record));
        assertEquals("9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d", record.employeeId);
        assertEquals("1a2bc3de-4f5g-6h7i-8j9k-0l1m2n3o4p5q", record.managerId);
      }







    @Test
    void csvMissingValuesBecomeDefaults() {
        BeamProcessor.EmployeeRecord r = new BeamProcessor.EmployeeRecord();
        BeamProcessor.setDefaults(r, "execution-1", "source");
        assertTrue(BeamProcessor.parseCsvRow("EMP1,,,,", r));
        assertEquals("EMP1", r.employeeId);
        assertEquals(" ", r.firstName);
        assertEquals("MA==", r.salary);
    }





}
