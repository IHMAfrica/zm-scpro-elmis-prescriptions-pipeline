package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.PrescriptionMessage;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;
import zm.gov.moh.hie.scp.sink.PostgresPrescriptionSink;
import java.math.BigDecimal;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final Config cfg = Config.fromEnvAndArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrapServers)
                .setTopics(cfg.kafkaTopic)
                .setGroupId(cfg.kafkaGroupId)
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "2000")
                .setProperty("max.poll.interval.ms", "10000")
                .setProperty("max.poll.records", "50")
                .setProperty("request.timeout.ms", "2540000")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("default.api.timeout.ms", "2540000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();


        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-prescriptions"
        ).startNewChain();

        SingleOutputStreamOperator<PrescriptionRecord> records = kafkaStream
                .filter(s -> !StringUtils.isNullOrWhitespaceOnly(s))
                .flatMap(new JsonToPrescriptionRecordFlatMapFunction())
                .filter(record -> !StringUtils.isNullOrWhitespaceOnly(record.hmisCode))
                .name("parse-and-flatten")
                .disableChaining();

        records.addSink(
                new PostgresPrescriptionSink(
                        cfg.jdbcUrl,
                        cfg.jdbcUser,
                        cfg.jdbcPassword,
                        "crt.prescription"
                )
        ).name("postgres-sink");

        env.execute("SC eLMIS Prescriptions Pipeline");
    }

    public static class JsonToPrescriptionRecordFlatMapFunction extends RichFlatMapFunction<String, PrescriptionRecord> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public void flatMap(String value, Collector<PrescriptionRecord> out) throws Exception {
            try {
                JsonNode msg = mapper.readTree(value);

                // Extract base fields from root and msh
                String prescriptionUuid = msg.get("prescriptionUuid") != null ? msg.get("prescriptionUuid").asText() : null;
                String messageId = msg.get("msh") != null && msg.get("msh").get("messageId") != null ?
                        msg.get("msh").get("messageId").asText() : null;
                String hmisCode = msg.get("msh") != null && msg.get("msh").get("hmisCode") != null ?
                        msg.get("msh").get("hmisCode").asText() : null;
                String mflCode = msg.get("msh") != null && msg.get("msh").get("mflCode") != null ?
                        msg.get("msh").get("mflCode").asText() : null;
                String mshTimestamp = msg.get("msh") != null && msg.get("msh").get("timestamp") != null ?
                        msg.get("msh").get("timestamp").asText() : null;
                String patientGuid = msg.get("patientUuid") != null ? msg.get("patientUuid").asText() : null;
                String artNumber = msg.get("artNumber") != null ? msg.get("artNumber").asText() : null;
                String cd4 = msg.get("cd4") != null ? msg.get("cd4").asText() : null;
                String viralLoad = msg.get("viralLoad") != null ? msg.get("viralLoad").asText() : null;
                String dateOfBled = msg.get("dateOfBled") != null ? msg.get("dateOfBled").asText() : null;
                Integer regimenId = msg.get("regimenId") != null ? msg.get("regimenId").asInt() : null;

                // Process prescriptionDrugs array (nested under prescription.prescriptionDrugs)
                JsonNode prescriptionNode = msg.get("prescription");
                JsonNode drugsNode = null;
                if (prescriptionNode != null) {
                    drugsNode = prescriptionNode.get("prescriptionDrugs");
                }

                if (drugsNode != null && drugsNode.isArray()) {
                    for (JsonNode drug : drugsNode) {
                        String medicationId = drug.get("medicationId") != null ? drug.get("medicationId").asText() : null;
                        String drugCode = drug.get("drugCode") != null ? drug.get("drugCode").asText() : null;
                        BigDecimal quantityPerDose = drug.get("quantityPerDose") != null ?
                                new BigDecimal(drug.get("quantityPerDose").asText()) : null;
                        String dosageUnit = drug.get("dosageUnit") != null ? drug.get("dosageUnit").asText() : null;
                        String frequency = drug.get("frequency") != null ? drug.get("frequency").asText() : null;
                        Integer duration = drug.get("duration") != null ? drug.get("duration").asInt() : null;

                        PrescriptionRecord record = new PrescriptionRecord(
                                prescriptionUuid,
                                messageId,
                                "drug",  // type
                                hmisCode,
                                mshTimestamp,
                                patientGuid,
                                artNumber,
                                mflCode,
                                cd4,
                                viralLoad,
                                dateOfBled,
                                regimenId,
                                null,  // regimenCode (not applicable for drugs)
                                duration,
                                medicationId,
                                drugCode,
                                quantityPerDose,
                                frequency,
                                dosageUnit
                        );
                        out.collect(record);
                    }
                }

                // Process regimen
                JsonNode regimenNode = msg.get("regimen");
                if (regimenNode != null && !regimenNode.isNull()) {
                    String regimenMedicationId = regimenNode.get("medicationId") != null ?
                            regimenNode.get("medicationId").asText() : null;
                    if (regimenMedicationId != null && !regimenMedicationId.isEmpty()) {
                        String regimenCode = regimenNode.get("regimenCode") != null ?
                                regimenNode.get("regimenCode").asText() : null;
                        BigDecimal quantityPerDose = regimenNode.get("quantityPerDose") != null ?
                                new BigDecimal(regimenNode.get("quantityPerDose").asText()) : null;
                        String dosageUnit = regimenNode.get("dosageUnit") != null ?
                                regimenNode.get("dosageUnit").asText() : null;
                        String frequency = regimenNode.get("frequency") != null ?
                                regimenNode.get("frequency").asText() : null;
                        Integer duration = regimenNode.get("duration") != null ?
                                regimenNode.get("duration").asInt() : null;

                        PrescriptionRecord record = new PrescriptionRecord(
                                prescriptionUuid,
                                messageId,
                                "regimen",  // type
                                hmisCode,
                                mshTimestamp,
                                patientGuid,
                                artNumber,
                                mflCode,
                                cd4,
                                viralLoad,
                                dateOfBled,
                                regimenId,
                                regimenCode,
                                duration,
                                regimenMedicationId,
                                null,  // drugCode (not applicable for regimen)
                                quantityPerDose,
                                frequency,
                                dosageUnit
                        );
                        out.collect(record);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error flattening prescription record: {}", value, e);
            }
        }
    }
}