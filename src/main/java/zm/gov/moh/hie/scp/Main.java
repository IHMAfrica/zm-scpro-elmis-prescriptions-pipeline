package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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
                .map(new JsonToPrescriptionRecordMapFunction())
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

    private static class JsonToPrescriptionRecordMapFunction extends RichMapFunction<String, PrescriptionRecord> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public PrescriptionRecord map(String value) {
            try {
                PrescriptionMessage msg = mapper.readValue(value, PrescriptionMessage.class);
                String prescriptionUuid = msg.prescriptionUuid;
                String mshTimestamp = msg.msh != null ? msg.msh.timestamp : null;
                String hmisCode = msg.msh != null ? msg.msh.hmisCode : null;

                if (StringUtils.isNullOrWhitespaceOnly(hmisCode)) {
                    hmisCode = msg.msh != null ? msg.msh.mflCode : null;
                }

                int drugCount = msg.prescription != null ? msg.prescription.prescriptionDrugs.size() : 0;
                int regimenCount = msg.regimen != null && msg.regimen.quantityPerDose != null ? msg.regimen.quantityPerDose.intValue() : 0;

                // Extract new fields
                String patientGuid = (msg.patientUuid != null && !msg.patientUuid.isBlank()) ? msg.patientUuid : null;
                String artNumber = (msg.artNumber != null && !msg.artNumber.isBlank()) ? msg.artNumber : null;
                String mflCode = msg.msh != null ? msg.msh.mflCode : null;
                String cd4 = (msg.cd4 != null && !msg.cd4.isBlank()) ? msg.cd4 : null;
                String viralLoad = (msg.viralLoad != null && !msg.viralLoad.isBlank()) ? msg.viralLoad : null;
                String dateOfBled = (msg.dateOfBled != null && !msg.dateOfBled.isBlank()) ? msg.dateOfBled : null;
                Integer regimenId = msg.regimenId;

                String regimenCode = null;
                Integer duration = null;
                String medicationId = null;
                BigDecimal unitQtyPerDose = null;
                String frequency = null;
                String unitOfMeasurement = null;

                if (msg.regimen != null) {
                    regimenCode = (msg.regimen.regimenCode != null && !msg.regimen.regimenCode.isBlank()) ? msg.regimen.regimenCode : null;
                    duration = msg.regimen.duration;
                    medicationId = (msg.regimen.medicationId != null && !msg.regimen.medicationId.isBlank()) ? msg.regimen.medicationId : null;
                    unitQtyPerDose = msg.regimen.quantityPerDose;
                    frequency = (msg.regimen.frequency != null && !msg.regimen.frequency.isBlank()) ? msg.regimen.frequency : null;
                    unitOfMeasurement = (msg.regimen.dosageUnit != null && !msg.regimen.dosageUnit.isBlank()) ? msg.regimen.dosageUnit : null;
                }

                return new PrescriptionRecord(prescriptionUuid, hmisCode, mshTimestamp, drugCount, regimenCount,
                        patientGuid, artNumber, mflCode, cd4, viralLoad,
                        dateOfBled, regimenId, regimenCode, duration,
                        medicationId, unitQtyPerDose, frequency, unitOfMeasurement);
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", value, e);
                return new PrescriptionRecord(null, null, null, 0, 0,
                        null, null, null, null, null,
                        null, null, null, null,
                        null, null, null, null);
            }
        }
    }
}