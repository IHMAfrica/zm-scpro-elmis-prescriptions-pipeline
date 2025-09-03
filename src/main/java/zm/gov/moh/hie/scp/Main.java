package zm.gov.moh.hie.scp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.PrescriptionMessage;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;
import zm.gov.moh.hie.scp.sink.PostgresPrescriptionSink;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // Read config from env or system properties
        String kafkaBrokers = getConf("KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers", "154.120.216.119:9093,102.23.120.153:9093,102.23.123.251:9093");
        String kafkaTopic = getConf("KAFKA_TOPIC", "kafka.topic", "prescriptions");
        String kafkaGroup = getConf("KAFKA_GROUP_ID", "kafka.group.id", "prescriptions-consumer");
        String kafkaSecProtocol = getConf("KAFKA_SECURITY_PROTOCOL", "kafka.security.protocol", "SASL_PLAINTEXT");
        String saslMechanism = getConf("KAFKA_SASL_MECHANISM", "kafka.sask.mechanism", "SCRAM-SHA-256");
        String kafkaSaslUsername = getConf("KAFKA_SASL_USERNAME", "kafka.sasl.username", "admin");
        String kafkaSaslPassword = getConf("KAFKA_SASL_PASSWORD", "kafka.sasl.password", "075F80FED7C6");

        String pgUrl = getConf("POSTGRES_URL", "postgres.url", "jdbc:postgresql://localhost:5432/postgres");
        String pgUser = getConf("POSTGRES_USER", "postgres.user", "postgres");
        String pgPass = getConf("POSTGRES_PASSWORD", "postgres.password", "postgres");
        String pgTable = getConf("POSTGRES_TABLE", "postgres.table", "prescriptions");

        LOG.info("Starting pipeline. Kafka: {} topic={} group={}, Postgres: {} table={} user={}", kafkaBrokers, kafkaTopic, kafkaGroup, pgUrl, pgTable, pgUser);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", kafkaSecProtocol)
                .setProperty("sasl.mechanism", saslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + kafkaSaslUsername + "\" " +
                                "password=\"" + kafkaSaslPassword + "\";")
                .build();

        DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-prescriptions");

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SingleOutputStreamOperator<PrescriptionRecord> records = kafkaStream
                .filter(s -> !StringUtils.isNullOrWhitespaceOnly(s))
                .map(value -> toRecord(value, mapper))
                .name("parse-and-flatten");

        records.addSink(new PostgresPrescriptionSink(pgUrl, pgUser, pgPass, pgTable)).name("postgres-sink");

        env.execute("SC ELMIS Prescriptions Pipeline");
    }

    private static String getConf(String envKey, String sysKey, String defaultVal) {
        String v = System.getenv(envKey);
        if (!StringUtils.isNullOrWhitespaceOnly(v)) return v;
        v = System.getProperty(sysKey);
        if (!StringUtils.isNullOrWhitespaceOnly(v)) return v;
        return defaultVal;
    }

    private static PrescriptionRecord toRecord(String json, ObjectMapper mapper) {
        try {
            PrescriptionMessage msg = mapper.readValue(json, PrescriptionMessage.class);
            String messageId = msg.msh != null ? msg.msh.messageId : null;
            String mshTimestamp = msg.msh != null ? msg.msh.timestamp : null;
            String patientUuid = msg.patientUuid;
            String artNumber = msg.artNumber;
            String cd4 = msg.cd4;
            String viralLoad = msg.viralLoad;
            String dateOfBled = msg.dateOfBled;
            Integer regimenId = msg.regimenId;
            String clinicianName = msg.clinicianName;
            String prescriptionUuid = msg.prescriptionUuid;
            String prescriptionDate = (msg.prescription != null) ? msg.prescription.date : null;
            return new PrescriptionRecord(messageId, mshTimestamp, patientUuid, artNumber, cd4, viralLoad, dateOfBled, regimenId, clinicianName, prescriptionUuid, prescriptionDate);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse JSON: {}", json, e);
            // Drop a malformed message by returning an empty record or null; here we return a record with minimal info
            return new PrescriptionRecord(null, null, null, null, null, null, null, null, null, null, null);
        }
    }
}