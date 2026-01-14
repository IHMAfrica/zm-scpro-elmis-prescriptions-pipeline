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
                    hmisCode = msg.msh.mflCode;
                }

                int drugCount = msg.prescription != null ? msg.prescription.prescriptionDrugs.size() : 0;
                int regimenCount = msg.regimen != null ? msg.regimen.quantityPerDose : 0;

                return new PrescriptionRecord(prescriptionUuid, hmisCode, mshTimestamp, drugCount, regimenCount);
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", value, e);
                return new PrescriptionRecord(null, null, null, 0, 0);
            }
        }
    }
}