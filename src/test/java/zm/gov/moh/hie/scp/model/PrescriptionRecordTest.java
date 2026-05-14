package zm.gov.moh.hie.scp.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

public class PrescriptionRecordTest {

    @Test
    public void testConstructorAcceptsTypeParameter() {
        // Arrange
        String prescriptionUuid = "uuid-123";
        String messageId = "msg-456";
        String type = "drug";
        String hmisCode = "hmis-789";
        String mshTimestamp = "2024-01-01T12:00:00";
        String patientGuid = "patient-guid";
        String artNumber = "art-123";
        String mflCode = "mfl-456";
        String cd4 = "500";
        String viralLoad = "1000";
        String dateOfBled = "2024-01-01";
        Integer regimenId = 1;
        String regimenCode = "regimen-001";
        Integer duration = 30;
        String medicationId = "med-123";
        BigDecimal unitQtyPerDose = new BigDecimal("1.0");
        String frequency = "twice daily";
        String unitOfMeasurement = "tablet";

        // Act
        PrescriptionRecord record = new PrescriptionRecord(
                prescriptionUuid, messageId, type, hmisCode, mshTimestamp,
                patientGuid, artNumber, mflCode, cd4, viralLoad,
                dateOfBled, regimenId, regimenCode, duration,
                medicationId, "drug-code-123", unitQtyPerDose, frequency, unitOfMeasurement
        );

        // Assert
        assertEquals(type, record.type);
    }

    @Test
    public void testGetTypeReturnsCorrectValue() {
        // Arrange
        PrescriptionRecord record = new PrescriptionRecord();
        String expectedType = "regimen";

        // Act
        record.setType(expectedType);
        String actualType = record.getType();

        // Assert
        assertEquals(expectedType, actualType);
    }

    @Test
    public void testSetTypeUpdatesValue() {
        // Arrange
        PrescriptionRecord record = new PrescriptionRecord();
        String type1 = "drug";
        String type2 = "regimen";

        // Act
        record.setType(type1);
        assertEquals(type1, record.getType());

        record.setType(type2);

        // Assert
        assertEquals(type2, record.getType());
    }

    @Test
    public void testConstructorWithDrugType() {
        // Arrange & Act
        PrescriptionRecord record = new PrescriptionRecord(
                "uuid-1", "msg-1", "drug", "hmis-1", "2024-01-01T12:00:00",
                "patient-1", "art-1", "mfl-1", "500", "1000",
                "2024-01-01", 1, "regimen-1", 30,
                "med-1", "CODE1", new BigDecimal("1.0"), "daily", "tablet"
        );

        // Assert
        assertEquals("drug", record.getType());
    }
}
