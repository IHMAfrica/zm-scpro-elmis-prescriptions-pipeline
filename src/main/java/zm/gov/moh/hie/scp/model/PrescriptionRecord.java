package zm.gov.moh.hie.scp.model;

import java.io.Serializable;

public class PrescriptionRecord implements Serializable {
    public String messageId;
    public String hmisCode;
    public String mshTimestamp;
    public int drugCount;
    public int regimenCount;

    public PrescriptionRecord() {}

    public PrescriptionRecord(String messageId, String hmisCode, String mshTimestamp, int drugCount, int regimenCount) {
        this.messageId = messageId;
        this.hmisCode = hmisCode;
        this.mshTimestamp = mshTimestamp;
        this.drugCount = drugCount;
        this.regimenCount = regimenCount;
    }
}
