package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

public class PrescriptionRecord implements Serializable {
    public String messageId;
    public String mshTimestamp;
    public String patientUuid;
    public String artNumber;
    public String cd4;
    public String viralLoad;
    public String dateOfBled;
    public Integer regimenId;
    public String clinicianName;
    public String prescriptionUuid;
    public String prescriptionDate;

    public PrescriptionRecord() {}

    public PrescriptionRecord(String messageId, String mshTimestamp, String patientUuid, String artNumber,
                              String cd4, String viralLoad, String dateOfBled, Integer regimenId,
                              String clinicianName, String prescriptionUuid, String prescriptionDate) {
        this.messageId = messageId;
        this.mshTimestamp = mshTimestamp;
        this.patientUuid = patientUuid;
        this.artNumber = artNumber;
        this.cd4 = cd4;
        this.viralLoad = viralLoad;
        this.dateOfBled = dateOfBled;
        this.regimenId = regimenId;
        this.clinicianName = clinicianName;
        this.prescriptionUuid = prescriptionUuid;
        this.prescriptionDate = prescriptionDate;
    }
}
