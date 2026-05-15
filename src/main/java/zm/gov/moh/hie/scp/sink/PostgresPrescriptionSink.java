package zm.gov.moh.hie.scp.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;
import zm.gov.moh.hie.scp.util.DateTimeUtil;
import java.sql.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

public class PostgresPrescriptionSink extends RichSinkFunction<PrescriptionRecord> {
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String table;

    private transient Connection connection;
    private transient PreparedStatement insertStmt;

    public PostgresPrescriptionSink(String jdbcUrl, String user, String password, String table) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(true);
        String insertQuery = "INSERT INTO "+ table + "(" +
                "uuid, message_id, type, hmis_code, date, \"time\", " +
                "patient_guid, art_number, mfl_code, cd4, viral_load, date_of_bled, " +
                "regimen_id, regimen_code, duration, medication_id, drug_code, unit_qty_per_dose, frequency, unit_of_measurement, msh_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (uuid, medication_id, type) " +
                "DO UPDATE SET " +
                "message_id = EXCLUDED.message_id, " +
                "hmis_code = EXCLUDED.hmis_code, " +
                "date = CURRENT_DATE, " +
                "\"time\" = CURRENT_TIME::time(0), " +
                "patient_guid = EXCLUDED.patient_guid, " +
                "art_number = EXCLUDED.art_number, " +
                "mfl_code = EXCLUDED.mfl_code, " +
                "cd4 = EXCLUDED.cd4, " +
                "viral_load = EXCLUDED.viral_load, " +
                "date_of_bled = EXCLUDED.date_of_bled, " +
                "regimen_id = EXCLUDED.regimen_id, " +
                "regimen_code = EXCLUDED.regimen_code, " +
                "duration = EXCLUDED.duration, " +
                "drug_code = EXCLUDED.drug_code, " +
                "unit_qty_per_dose = EXCLUDED.unit_qty_per_dose, " +
                "frequency = EXCLUDED.frequency, " +
                "unit_of_measurement = EXCLUDED.unit_of_measurement, " +
                "msh_timestamp = EXCLUDED.msh_timestamp";

        insertStmt = connection.prepareStatement(insertQuery);
    }

    @Override
    public void invoke(PrescriptionRecord value, Context context) throws Exception {
        insertStmt.setString(1, value.prescriptionUuid);
        insertStmt.setString(2, value.messageId);
        insertStmt.setString(3, value.type);
        insertStmt.setString(4, value.hmisCode);

        LocalDateTime timestamp = LocalDateTime.parse(value.mshTimestamp, DateTimeUtil.TIMESTAMP_FORMATTER);
        Timestamp ts = Timestamp.valueOf(timestamp);
        insertStmt.setDate(5, new java.sql.Date(ts.getTime()));
        insertStmt.setTime(6, new java.sql.Time(ts.getTime()));

        insertStmt.setString(7, value.patientGuid);
        insertStmt.setString(8, value.artNumber);
        insertStmt.setString(9, value.mflCode);
        insertStmt.setString(10, value.cd4);
        insertStmt.setString(11, value.viralLoad);

        // dateOfBled - parse with try/catch
        Timestamp dateOfBledTs = null;
        if (value.dateOfBled != null && !value.dateOfBled.isBlank()) {
            try {
                LocalDateTime dateOfBledLdt = LocalDateTime.parse(value.dateOfBled, DateTimeUtil.TIMESTAMP_FORMATTER);
                dateOfBledTs = Timestamp.valueOf(dateOfBledLdt);
            } catch (Exception e) {
                // Leave null
            }
        }
        if (dateOfBledTs != null) {
            insertStmt.setTimestamp(12, dateOfBledTs);
        } else {
            insertStmt.setNull(12, Types.TIMESTAMP);
        }

        // regimenId
        if (value.regimenId != null) {
            insertStmt.setInt(13, value.regimenId);
        } else {
            insertStmt.setNull(13, Types.INTEGER);
        }

        insertStmt.setString(14, value.regimenCode);

        // duration
        if (value.duration != null) {
            insertStmt.setInt(15, value.duration);
        } else {
            insertStmt.setNull(15, Types.INTEGER);
        }

        insertStmt.setString(16, value.medicationId);
        insertStmt.setString(17, value.drugCode);
        insertStmt.setBigDecimal(18, value.unitQtyPerDose);
        insertStmt.setString(19, value.frequency);
        insertStmt.setString(20, value.unitOfMeasurement);
        insertStmt.setTimestamp(21, ts);

        insertStmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
    }
}
