package casestudy.lumi.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IngestionControllerTest {
    @Test void missingFileLocationReturnsBadRequest(){
        IngestionController c=new IngestionController(new RestTemplate()); var response=c.triggerIngestion(Map.of());
        assertEquals(HttpStatus.BAD_REQUEST,response.getStatusCode()); assertEquals("FAILED",response.getBody().get("status"));
    }
    @Test void missingControlFileReturnsBadRequest() throws Exception {
        IngestionController c=new IngestionController(new RestTemplate()); Path source=Files.createTempFile("lumi-source",".json"); Files.writeString(source,"{}\n");
        var response=c.triggerIngestion(Map.of("file_location",source.toString()));
        assertEquals(HttpStatus.BAD_REQUEST,response.getStatusCode()); assertTrue(response.getBody().get("message").toString().contains("control_file_location"));
    }
    @Test void validControlFileIsRead() throws Exception {
        IngestionController c=new IngestionController(new RestTemplate()); Path control=Files.createTempFile("lumi-control",".properties"); Files.writeString(control,"record_count=3\n");
        assertEquals(3,c.readExpectedRecordCount(control));
    }
    @Test void invalidControlFileIsRejected() throws Exception {
        IngestionController c=new IngestionController(new RestTemplate()); Path control=Files.createTempFile("lumi-control",".properties"); Files.writeString(control,"record_count=abc\n");
        assertThrows(IllegalArgumentException.class,()->c.readExpectedRecordCount(control));
    }
    @Test void negativeControlFileIsRejected() throws Exception {
        IngestionController c=new IngestionController(new RestTemplate()); Path control=Files.createTempFile("lumi-control",".properties"); Files.writeString(control,"record_count=-1\n");
        assertThrows(IllegalArgumentException.class,()->c.readExpectedRecordCount(control));
    }
    @Test void thresholdControlsSplitting() throws Exception {
        IngestionController c=new IngestionController(new RestTemplate());
        Field f=IngestionController.class.getDeclaredField("fileSizeThreshold"); f.setAccessible(true); f.setLong(c,200);
        assertFalse(c.shouldSplit(200)); assertTrue(c.shouldSplit(201));
    }
    @Test void supportedFormatsAreDetected() {
        IngestionController c=new IngestionController(new RestTemplate());
        assertEquals("json",c.detectFileFormat("x.json")); assertEquals("json",c.detectFileFormat("x.jsonl")); assertEquals("csv",c.detectFileFormat("x.csv"));
        assertThrows(IllegalArgumentException.class, () -> c.detectFileFormat("x.txt"));
    }
}
