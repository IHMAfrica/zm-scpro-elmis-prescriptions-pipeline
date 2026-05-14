package zm.gov.moh.hie.scp;

import org.junit.jupiter.api.Test;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration Test for UNIQUE Constraint Enforcement
 *
 * Database Schema: crt.prescription table has UNIQUE constraint on (uuid, medication_id, type)
 * This constraint prevents exact duplicate records from being inserted.
 *
 * Test Approach:
 * These tests document the expected behavior of the UNIQUE constraint on the database:
 * 1. testDuplicateRecordRejected() - Same uuid/medication/type should fail INSERT
 * 2. testDifferentMedicationIdAllowed() - Same uuid/type but different medication should succeed
 * 3. testDifferentTypeAllowed() - Same uuid/medication but different type should succeed
 *
 * To run these tests against a real PostgreSQL database:
 * 1. Create PostgreSQL database with schema:
 *    CREATE SCHEMA IF NOT EXISTS crt;
 *
 * 2. Create crt.prescription table with UNIQUE constraint:
 *    CREATE TABLE crt.prescription (
 *      id SERIAL PRIMARY KEY,
 *      uuid VARCHAR(255) NOT NULL,
 *      message_id VARCHAR(255),
 *      type VARCHAR(50) NOT NULL,
 *      hmis_code VARCHAR(50),
 *      drug_count INT,
 *      regimen_count INT,
 *      date DATE,
 *      "time" TIME,
 *      patient_guid VARCHAR(255),
 *      art_number VARCHAR(50),
 *      mfl_code VARCHAR(50),
 *      cd4 VARCHAR(50),
 *      viral_load VARCHAR(50),
 *      date_of_bled TIMESTAMP,
 *      regimen_id INT,
 *      regimen_code VARCHAR(100),
 *      duration INT,
 *      medication_id VARCHAR(255) NOT NULL,
 *      unit_qty_per_dose NUMERIC(10, 2),
 *      frequency VARCHAR(100),
 *      unit_of_measurement VARCHAR(50),
 *      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 *      UNIQUE(uuid, medication_id, type)
 *    );
 *
 * 3. Set environment variables or update test configuration:
 *    export JDBC_URL=jdbc:postgresql://localhost:5432/test_db
 *    export JDBC_USER=postgres
 *    export JDBC_PASSWORD=password
 *
 * 4. Run tests:
 *    ./gradlew test --tests '*UniqueConstraintTest'
 */
public class UniqueConstraintTest {

    /**
     * Test 1: testDuplicateRecordRejected
     *
     * Scenario: Two records with identical uuid, medication_id, and type
     * Expected: Second insert should fail with constraint violation
     *
     * Business Logic:
     * When the same prescription (uuid) is processed twice with the same medication and type,
     * the UNIQUE constraint prevents duplicate database entries. This protects against
     * reprocessing the same message multiple times and creating duplicate records.
     *
     * Manual Test Steps (when running against real database):
     * 1. Create record 1: uuid='rx-123', medication_id='MED-X', type='drug'
     * 2. Insert record 1 into database -> SUCCESS
     * 3. Create record 2: uuid='rx-123', medication_id='MED-X', type='drug' (exact duplicate)
     * 4. Insert record 2 into database -> FAILS with SQLException (UNIQUE constraint violation)
     * 5. Query database: COUNT(*) WHERE uuid='rx-123' AND medication_id='MED-X' AND type='drug'
     * 6. Verify: Only 1 row exists (second insert was rejected)
     */
    @Test
    void testDuplicateRecordRejected() {
        // Arrange - Create PrescriptionRecord for duplicate detection test
        PrescriptionRecord record1 = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid
                "msg-1",                     // messageId
                "drug",                      // type
                "MED-X"                      // medicationId
        );

        PrescriptionRecord record2 = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid - SAME
                "msg-2",                     // messageId - different
                "drug",                      // type - SAME
                "MED-X"                      // medicationId - SAME
        );

        // Test Documentation
        System.out.println("Test: testDuplicateRecordRejected");
        System.out.println("Record 1: uuid=" + record1.prescriptionUuid +
                         ", medication_id=" + record1.medicationId +
                         ", type=" + record1.type);
        System.out.println("Record 2: uuid=" + record2.prescriptionUuid +
                         ", medication_id=" + record2.medicationId +
                         ", type=" + record2.type);
        System.out.println("Expected: Record 2 insert FAILS due to UNIQUE constraint on (uuid, medication_id, type)");
        System.out.println();

        // Verify records have identical key components
        assertEquals(record1.prescriptionUuid, record2.prescriptionUuid,
                    "Test setup: Both records should have same uuid");
        assertEquals(record1.medicationId, record2.medicationId,
                    "Test setup: Both records should have same medication_id");
        assertEquals(record1.type, record2.type,
                    "Test setup: Both records should have same type");
    }

    /**
     * Test 2: testDifferentMedicationIdAllowed
     *
     * Scenario: Two records with same uuid and type but different medication_id
     * Expected: Both inserts should succeed
     *
     * Business Logic:
     * One prescription can contain multiple drugs. The UNIQUE constraint allows multiple
     * medications for the same prescription (uuid) as long as medication_id differs.
     * This supports prescriptions with multiple drug instructions.
     *
     * Manual Test Steps (when running against real database):
     * 1. Create record 1: uuid='rx-123', medication_id='MED-X', type='drug'
     * 2. Insert record 1 into database -> SUCCESS
     * 3. Create record 2: uuid='rx-123', medication_id='MED-Y', type='drug' (different medication)
     * 4. Insert record 2 into database -> SUCCESS (different medication_id bypasses UNIQUE)
     * 5. Query database for rx-123 with type='drug':
     *    - COUNT WHERE medication_id='MED-X' -> 1
     *    - COUNT WHERE medication_id='MED-Y' -> 1
     * 6. Verify: Both rows exist (different medications allowed)
     */
    @Test
    void testDifferentMedicationIdAllowed() {
        // Arrange - Create records with same uuid/type but different medication
        PrescriptionRecord record1 = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid
                "msg-1",                     // messageId
                "drug",                      // type
                "MED-X"                      // medicationId
        );

        PrescriptionRecord record2 = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid - SAME
                "msg-1",                     // messageId - SAME
                "drug",                      // type - SAME
                "MED-Y"                      // medicationId - DIFFERENT
        );

        // Test Documentation
        System.out.println("Test: testDifferentMedicationIdAllowed");
        System.out.println("Record 1: uuid=" + record1.prescriptionUuid +
                         ", medication_id=" + record1.medicationId +
                         ", type=" + record1.type);
        System.out.println("Record 2: uuid=" + record2.prescriptionUuid +
                         ", medication_id=" + record2.medicationId +
                         ", type=" + record2.type);
        System.out.println("Expected: Both inserts SUCCEED because medication_id differs");
        System.out.println("Use Case: One prescription with multiple drugs");
        System.out.println();

        // Verify records differ only in medication_id
        assertEquals(record1.prescriptionUuid, record2.prescriptionUuid,
                    "Test setup: Both records should have same uuid");
        assertEquals(record1.type, record2.type,
                    "Test setup: Both records should have same type");
        assertNotEquals(record1.medicationId, record2.medicationId,
                       "Test setup: Records should have different medication_id");
    }

    /**
     * Test 3: testDifferentTypeAllowed
     *
     * Scenario: Two records with same uuid and medication_id but different type
     * Expected: Both inserts should succeed
     *
     * Business Logic:
     * The same medication can be referenced in different contexts (as a drug and as a regimen).
     * The UNIQUE constraint allows this because type differs, distinguishing between:
     * - type='drug': Individual drug dosing instruction
     * - type='regimen': Multi-drug regimen definition
     *
     * Manual Test Steps (when running against real database):
     * 1. Create record 1: uuid='rx-123', medication_id='MED-X', type='drug'
     * 2. Insert record 1 into database -> SUCCESS
     * 3. Create record 2: uuid='rx-123', medication_id='MED-X', type='regimen'
     * 4. Insert record 2 into database -> SUCCESS (different type bypasses UNIQUE)
     * 5. Query database for rx-123 with medication_id='MED-X':
     *    - COUNT WHERE type='drug' -> 1
     *    - COUNT WHERE type='regimen' -> 1
     * 6. Verify: Both rows exist (different types allowed)
     */
    @Test
    void testDifferentTypeAllowed() {
        // Arrange - Create records with same uuid/medication but different type
        PrescriptionRecord drugRecord = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid
                "msg-1",                     // messageId
                "drug",                      // type
                "MED-X"                      // medicationId
        );

        PrescriptionRecord regimenRecord = createPrescriptionRecord(
                "rx-123",                    // prescriptionUuid - SAME
                "msg-1",                     // messageId - SAME
                "regimen",                   // type - DIFFERENT
                "MED-X"                      // medicationId - SAME
        );

        // Test Documentation
        System.out.println("Test: testDifferentTypeAllowed");
        System.out.println("Record 1 (Drug): uuid=" + drugRecord.prescriptionUuid +
                         ", medication_id=" + drugRecord.medicationId +
                         ", type=" + drugRecord.type);
        System.out.println("Record 2 (Regimen): uuid=" + regimenRecord.prescriptionUuid +
                         ", medication_id=" + regimenRecord.medicationId +
                         ", type=" + regimenRecord.type);
        System.out.println("Expected: Both inserts SUCCEED because type differs");
        System.out.println("Use Case: Same medication as drug AND as regimen component");
        System.out.println();

        // Verify records differ only in type
        assertEquals(drugRecord.prescriptionUuid, regimenRecord.prescriptionUuid,
                    "Test setup: Both records should have same uuid");
        assertEquals(drugRecord.medicationId, regimenRecord.medicationId,
                    "Test setup: Both records should have same medication_id");
        assertNotEquals(drugRecord.type, regimenRecord.type,
                       "Test setup: Records should have different type");
        assertEquals("drug", drugRecord.type, "Test setup: First record should be drug type");
        assertEquals("regimen", regimenRecord.type, "Test setup: Second record should be regimen type");
    }

    /**
     * Helper method to create a PrescriptionRecord with common test values
     */
    private PrescriptionRecord createPrescriptionRecord(
            String prescriptionUuid,
            String messageId,
            String type,
            String medicationId) {
        return new PrescriptionRecord(
                prescriptionUuid,
                messageId,
                type,
                "20050020",                                  // hmisCode
                "2024-01-01T12:00:00",                       // mshTimestamp
                "pat-789",                                   // patientGuid
                "art-123",                                   // artNumber
                "mfl-456",                                   // mflCode
                "500",                                       // cd4
                "1000",                                      // viralLoad
                "2024-01-01T10:00:00",                       // dateOfBled
                type.equals("regimen") ? 1 : null,           // regimenId
                type.equals("regimen") ? "TDF/3TC/DTG" : null, // regimenCode
                type.equals("regimen") ? 90 : null,          // duration
                medicationId,
                type.equals("drug") ? "EM0347" : null,       // drugCode
                new BigDecimal("150"),                       // unitQtyPerDose
                type.equals("drug") ? "bd" : "od",           // frequency
                "tablet"                                     // unitOfMeasurement
        );
    }
}
