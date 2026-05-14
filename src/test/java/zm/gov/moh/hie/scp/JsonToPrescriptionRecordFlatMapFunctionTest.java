package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonToPrescriptionRecordFlatMapFunctionTest {
    private ObjectMapper mapper;
    private Main.JsonToPrescriptionRecordFlatMapFunction flatMapFunction;

    @BeforeEach
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        flatMapFunction = new Main.JsonToPrescriptionRecordFlatMapFunction();
        flatMapFunction.open(new Configuration());
    }

    /**
     * Mock collector that captures emitted records for testing
     */
    private static class MockCollector implements Collector<PrescriptionRecord> {
        private final List<PrescriptionRecord> records = new ArrayList<>();

        @Override
        public void collect(PrescriptionRecord record) {
            records.add(record);
        }

        @Override
        public void close() {
            // No-op for testing
        }

        public List<PrescriptionRecord> getRecords() {
            return records;
        }
    }

    /**
     * Test: Flatten drugs only
     * - Payload with 2 drugs, empty regimen
     * - Expected: 2 records with type='drug'
     */
    @Test
    public void testFlattenDrugsOnly() throws Exception {
        // Arrange - actual payload structure
        String testJson = "{\n" +
                "  \"msh\": {\n" +
                "    \"timestamp\": \"2026-05-14 14:46:02\",\n" +
                "    \"messageId\": \"msg-456\",\n" +
                "    \"hmisCode\": \"20050020\",\n" +
                "    \"mflCode\": \"20050020\"\n" +
                "  },\n" +
                "  \"prescriptionUuid\": \"rx-123\",\n" +
                "  \"patientUuid\": \"pat-789\",\n" +
                "  \"artNumber\": \"\",\n" +
                "  \"cd4\": \"150\",\n" +
                "  \"viralLoad\": \"100000\",\n" +
                "  \"dateOfBled\": \"1900-01-01 00:00:00\",\n" +
                "  \"regimenId\": 0,\n" +
                "  \"regimen\": {\n" +
                "    \"medicationId\": \"\",\n" +
                "    \"regimenCode\": \"\",\n" +
                "    \"quantityPerDose\": 0,\n" +
                "    \"dosageUnit\": \"\",\n" +
                "    \"frequency\": \"\",\n" +
                "    \"duration\": 0\n" +
                "  },\n" +
                "  \"prescription\": {\n" +
                "    \"prescriptionDrugs\": [\n" +
                "      {\"medicationId\": \"MED-1\", \"drugCode\": \"CODE1\", \"quantityPerDose\": 1.00, \"dosageUnit\": \"tab\", \"frequency\": \"bd\", \"duration\": 7},\n" +
                "      {\"medicationId\": \"MED-2\", \"drugCode\": \"CODE2\", \"quantityPerDose\": 1.00, \"dosageUnit\": \"tab\", \"frequency\": \"od\", \"duration\": 10}\n" +
                "    ],\n" +
                "    \"date\": \"2026-05-14\"\n" +
                "  }\n" +
                "}";

        MockCollector collector = new MockCollector();

        // Act
        flatMapFunction.flatMap(testJson, collector);

        // Assert
        List<PrescriptionRecord> records = collector.getRecords();
        assertEquals(2, records.size(), "Should emit 2 records for 2 drugs");

        // Verify first drug record
        PrescriptionRecord record1 = records.get(0);
        assertEquals("drug", record1.type, "First record should have type='drug'");
        assertEquals("MED-1", record1.medicationId, "First record should have medicationId=MED-1");
        assertEquals("rx-123", record1.prescriptionUuid, "First record should have correct prescriptionUuid");
        assertEquals("20050020", record1.hmisCode, "First record should have correct hmisCode");
        assertEquals("pat-789", record1.patientGuid, "First record should have correct patientGuid");
        assertEquals("tab", record1.unitOfMeasurement, "First record should have correct dosageUnit");

        // Verify second drug record
        PrescriptionRecord record2 = records.get(1);
        assertEquals("drug", record2.type, "Second record should have type='drug'");
        assertEquals("MED-2", record2.medicationId, "Second record should have medicationId=MED-2");
        assertEquals("rx-123", record2.prescriptionUuid, "Second record should have correct prescriptionUuid");
        assertEquals("20050020", record2.hmisCode, "Second record should have correct hmisCode");
        assertEquals("pat-789", record2.patientGuid, "Second record should have correct patientGuid");
    }

    /**
     * Test: Flatten regimen only
     * - Payload with regimen, empty prescriptionDrugs array
     * - Expected: 1 record with type='regimen'
     */
    @Test
    public void testFlattenRegimenOnly() throws Exception {
        // Arrange
        String testJson = "{\n" +
                "  \"msh\": {\n" +
                "    \"timestamp\": \"2026-05-14 14:48:12\",\n" +
                "    \"messageId\": \"msg-789\",\n" +
                "    \"hmisCode\": \"10090022\",\n" +
                "    \"mflCode\": \"10090022\"\n" +
                "  },\n" +
                "  \"prescriptionUuid\": \"rx-456\",\n" +
                "  \"patientUuid\": \"pat-101\",\n" +
                "  \"artNumber\": \"1010-022-2960774-5\",\n" +
                "  \"cd4\": \"\",\n" +
                "  \"viralLoad\": \"\",\n" +
                "  \"dateOfBled\": \"1900-01-01 00:00:00\",\n" +
                "  \"regimenId\": 1,\n" +
                "  \"regimen\": {\n" +
                "    \"medicationId\": \"REG-1\",\n" +
                "    \"regimenCode\": \"TDF / 3TC / DTG\",\n" +
                "    \"quantityPerDose\": 1.00,\n" +
                "    \"dosageUnit\": \"tab\",\n" +
                "    \"frequency\": \"od\",\n" +
                "    \"duration\": 90\n" +
                "  },\n" +
                "  \"prescription\": {\n" +
                "    \"prescriptionDrugs\": [],\n" +
                "    \"date\": \"\"\n" +
                "  }\n" +
                "}";

        MockCollector collector = new MockCollector();

        // Act
        flatMapFunction.flatMap(testJson, collector);

        // Assert
        List<PrescriptionRecord> records = collector.getRecords();
        assertEquals(1, records.size(), "Should emit 1 record for regimen");

        PrescriptionRecord record = records.get(0);
        assertEquals("regimen", record.type, "Record should have type='regimen'");
        assertEquals("REG-1", record.medicationId, "Record should have medicationId=REG-1");
        assertEquals("TDF / 3TC / DTG", record.regimenCode, "Record should have correct regimenCode");
        assertEquals("od", record.frequency, "Record should have correct frequency");
        assertEquals(90, record.duration, "Record should have correct duration");
        assertEquals("rx-456", record.prescriptionUuid, "Record should have correct prescriptionUuid");
        assertEquals("10090022", record.hmisCode, "Record should have correct hmisCode");
        assertEquals("pat-101", record.patientGuid, "Record should have correct patientGuid");
    }

    /**
     * Test: Flatten both drugs and regimen
     * - Payload with 2 drugs AND 1 regimen
     * - Expected: 3 records total (2 drug + 1 regimen)
     */
    @Test
    public void testFlattenBothDrugsAndRegimen() throws Exception {
        // Arrange
        String testJson = "{\n" +
                "  \"msh\": {\n" +
                "    \"timestamp\": \"2026-05-14 14:50:00\",\n" +
                "    \"messageId\": \"msg-012\",\n" +
                "    \"hmisCode\": \"20050022\",\n" +
                "    \"mflCode\": \"20050022\"\n" +
                "  },\n" +
                "  \"prescriptionUuid\": \"rx-789\",\n" +
                "  \"patientUuid\": \"pat-202\",\n" +
                "  \"artNumber\": \"ART-123\",\n" +
                "  \"cd4\": \"350\",\n" +
                "  \"viralLoad\": \"200000\",\n" +
                "  \"dateOfBled\": \"1900-01-01 00:00:00\",\n" +
                "  \"regimenId\": 1,\n" +
                "  \"regimen\": {\n" +
                "    \"medicationId\": \"REG-2\",\n" +
                "    \"regimenCode\": \"AZT/3TC/NVP\",\n" +
                "    \"quantityPerDose\": 1.00,\n" +
                "    \"dosageUnit\": \"tab\",\n" +
                "    \"frequency\": \"bd\",\n" +
                "    \"duration\": 180\n" +
                "  },\n" +
                "  \"prescription\": {\n" +
                "    \"prescriptionDrugs\": [\n" +
                "      {\"medicationId\": \"MED-3\", \"drugCode\": \"EM0347\", \"quantityPerDose\": 1.00, \"dosageUnit\": \"tab\", \"frequency\": \"bd\", \"duration\": 14},\n" +
                "      {\"medicationId\": \"MED-4\", \"drugCode\": \"EM1591\", \"quantityPerDose\": 1.00, \"dosageUnit\": \"tab\", \"frequency\": \"tds\", \"duration\": 21}\n" +
                "    ],\n" +
                "    \"date\": \"2026-05-14\"\n" +
                "  }\n" +
                "}";

        MockCollector collector = new MockCollector();

        // Act
        flatMapFunction.flatMap(testJson, collector);

        // Assert
        List<PrescriptionRecord> records = collector.getRecords();
        assertEquals(3, records.size(), "Should emit 3 records (2 drugs + 1 regimen)");

        // Verify drug records
        PrescriptionRecord record1 = records.get(0);
        assertEquals("drug", record1.type, "First record should have type='drug'");
        assertEquals("MED-3", record1.medicationId, "First record should have medicationId=MED-3");

        PrescriptionRecord record2 = records.get(1);
        assertEquals("drug", record2.type, "Second record should have type='drug'");
        assertEquals("MED-4", record2.medicationId, "Second record should have medicationId=MED-4");

        // Verify regimen record
        PrescriptionRecord record3 = records.get(2);
        assertEquals("regimen", record3.type, "Third record should have type='regimen'");
        assertEquals("REG-2", record3.medicationId, "Third record should have medicationId=REG-2");
        assertEquals("AZT/3TC/NVP", record3.regimenCode, "Third record should have correct regimenCode");
    }

    /**
     * Test: Patient context preservation
     * - Verify all emitted records preserve patient/facility context
     */
    @Test
    public void testPatientContextPreserved() throws Exception {
        // Arrange
        String testJson = "{\n" +
                "  \"msh\": {\n" +
                "    \"timestamp\": \"2026-05-14 14:46:02\",\n" +
                "    \"messageId\": \"msg-context\",\n" +
                "    \"hmisCode\": \"20050020\",\n" +
                "    \"mflCode\": \"20050020\"\n" +
                "  },\n" +
                "  \"prescriptionUuid\": \"rx-context\",\n" +
                "  \"patientUuid\": \"pat-context\",\n" +
                "  \"artNumber\": \"ART-001\",\n" +
                "  \"cd4\": \"250\",\n" +
                "  \"viralLoad\": \"150000\",\n" +
                "  \"dateOfBled\": \"2026-05-01 12:00:00\",\n" +
                "  \"regimenId\": 1,\n" +
                "  \"regimen\": {\n" +
                "    \"medicationId\": \"REG-CONTEXT\",\n" +
                "    \"regimenCode\": \"REG-CODE\",\n" +
                "    \"quantityPerDose\": 1.00,\n" +
                "    \"dosageUnit\": \"tab\",\n" +
                "    \"frequency\": \"od\",\n" +
                "    \"duration\": 90\n" +
                "  },\n" +
                "  \"prescription\": {\n" +
                "    \"prescriptionDrugs\": [\n" +
                "      {\"medicationId\": \"MED-X\", \"drugCode\": \"CODE-X\", \"quantityPerDose\": 1.00, \"dosageUnit\": \"tab\", \"frequency\": \"bd\", \"duration\": 7},\n" +
                "      {\"medicationId\": \"MED-Y\", \"drugCode\": \"CODE-Y\", \"quantityPerDose\": 2.00, \"dosageUnit\": \"cap\", \"frequency\": \"od\", \"duration\": 14}\n" +
                "    ],\n" +
                "    \"date\": \"2026-05-14\"\n" +
                "  }\n" +
                "}";

        MockCollector collector = new MockCollector();

        // Act
        flatMapFunction.flatMap(testJson, collector);

        // Assert
        List<PrescriptionRecord> records = collector.getRecords();
        assertEquals(3, records.size(), "Should emit 3 records (2 drugs + 1 regimen)");

        // Verify all records have same context
        for (PrescriptionRecord record : records) {
            assertEquals("rx-context", record.prescriptionUuid, "All records should have same prescriptionUuid");
            assertEquals("20050020", record.hmisCode, "All records should have same hmisCode");
            assertEquals("pat-context", record.patientGuid, "All records should have same patientGuid");
            assertEquals("250", record.cd4, "All records should have same cd4");
            assertEquals("150000", record.viralLoad, "All records should have same viralLoad");
            assertEquals("ART-001", record.artNumber, "All records should have same artNumber");
        }
    }
}
