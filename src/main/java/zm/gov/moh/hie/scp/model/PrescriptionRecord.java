package zm.gov.moh.hie.scp.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class PrescriptionRecord implements Serializable {
    public String prescriptionUuid;
    public String messageId;
    public String type;
    public String hmisCode;
    public String mshTimestamp;
    public String patientGuid;
    public String artNumber;
    public String mflCode;
    public String cd4;
    public String viralLoad;
    public String dateOfBled;
    public Integer regimenId;
    public String regimenCode;
    public Integer duration;
    public String medicationId;
    public String drugCode;
    public BigDecimal unitQtyPerDose;
    public String frequency;
    public String unitOfMeasurement;

    public PrescriptionRecord() {}

    public PrescriptionRecord(String prescriptionUuid, String messageId, String type, String hmisCode, String mshTimestamp,
                              String patientGuid, String artNumber, String mflCode, String cd4, String viralLoad,
                              String dateOfBled, Integer regimenId, String regimenCode, Integer duration,
                              String medicationId, String drugCode, BigDecimal unitQtyPerDose, String frequency, String unitOfMeasurement) {
        this.prescriptionUuid = prescriptionUuid;
        this.messageId = messageId;
        this.type = type;
        this.hmisCode = hmisCode;
        this.mshTimestamp = mshTimestamp;
        this.patientGuid = patientGuid;
        this.artNumber = artNumber;
        this.mflCode = mflCode;
        this.cd4 = cd4;
        this.viralLoad = viralLoad;
        this.dateOfBled = dateOfBled;
        this.regimenId = regimenId;
        this.regimenCode = regimenCode;
        this.duration = duration;
        this.medicationId = medicationId;
        this.drugCode = drugCode;
        this.unitQtyPerDose = unitQtyPerDose;
        this.frequency = frequency;
        this.unitOfMeasurement = unitOfMeasurement;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
