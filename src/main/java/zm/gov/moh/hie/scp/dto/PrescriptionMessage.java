package zm.gov.moh.hie.scp.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PrescriptionMessage {
    @JsonProperty("msh")
    public Msh msh;

    @JsonProperty("patientUuid")
    public String patientUuid;

    @JsonProperty("artNumber")
    public String artNumber;

    @JsonProperty("cd4")
    public String cd4;

    @JsonProperty("viralLoad")
    public String viralLoad;

    @JsonProperty("dateOfBled")
    public String dateOfBled;

    @JsonProperty("regimenId")
    public Integer regimenId;

    @JsonProperty("regimen")
    public Regimen regimen;

    @JsonProperty("vitals")
    public Vitals vitals;

    @JsonProperty("prescription")
    public Prescription prescription;

    @JsonProperty("clinicianName")
    public String clinicianName;

    @JsonProperty("prescriptionUuid")
    public String prescriptionUuid;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Msh {
        @JsonProperty("timestamp")
        public String timestamp;
        @JsonProperty("sendingApplication")
        public String sendingApplication;
        @JsonProperty("receivingApplication")
        public String receivingApplication;
        @JsonProperty("messageId")
        public String messageId;
        @JsonProperty("hmisCode")
        public String hmisCode;
        @JsonProperty("messageType")
        public String messageType;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Regimen {
        @JsonProperty("regimenCode")
        public String regimenCode;
        @JsonProperty("quantityPerDose")
        public Integer quantityPerDose;
        @JsonProperty("dosageUnit")
        public String dosageUnit;
        @JsonProperty("frequency")
        public String frequency;
        @JsonProperty("duration")
        public Integer duration;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Vitals {
        @JsonProperty("height")
        public String height;
        @JsonProperty("heightDateTimeCollected")
        public String heightDateTimeCollected;
        @JsonProperty("weight")
        public String weight;
        @JsonProperty("weightDateTimeCollected")
        public String weightDateTimeCollected;
        @JsonProperty("bloodPressure")
        public String bloodPressure;
        @JsonProperty("bloodPressureDateTimeCollected")
        public String bloodPressureDateTimeCollected;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Prescription {
        @JsonProperty("prescriptionDrugs")
        public List<PrescriptionDrug> prescriptionDrugs;
        @JsonProperty("date")
        public String date;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PrescriptionDrug {
        @JsonProperty("drugCode")
        public String drugCode;
        @JsonProperty("quantityPerDose")
        public Integer quantityPerDose;
        @JsonProperty("dosageUnit")
        public String dosageUnit;
        @JsonProperty("frequency")
        public String frequency;
        @JsonProperty("duration")
        public Integer duration;
    }
}
