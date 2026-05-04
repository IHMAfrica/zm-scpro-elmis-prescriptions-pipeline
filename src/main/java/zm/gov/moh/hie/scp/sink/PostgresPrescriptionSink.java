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
                "uuid, message_id, hmis_code, drug_count, regimen_count, date, \"time\", " +
                "patient_guid, art_number, mfl_code, cd4, viral_load, date_of_bled, " +
                "regimen_id, regimen_code, duration, medication_id, unit_qty_per_dose, frequency, unit_of_measurement) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        insertStmt = connection.prepareStatement(insertQuery);
    }

    @Override
    public void invoke(PrescriptionRecord value, Context context) throws Exception {
        // Original 6 parameters
        insertStmt.setString(1, value.prescriptionUuid);
        insertStmt.setString(2, value.messageId);
        insertStmt.setString(3, value.hmisCode);
        insertStmt.setInt(4, value.drugCount);
        insertStmt.setInt(5, value.regimenCount);

        LocalDateTime timestamp = LocalDateTime.parse(value.mshTimestamp, DateTimeUtil.TIMESTAMP_FORMATTER);
        Timestamp ts = Timestamp.valueOf(timestamp);
        insertStmt.setDate(6, new java.sql.Date(ts.getTime()));
        insertStmt.setTime(7, new java.sql.Time(ts.getTime()));

        // New 13 parameters
        insertStmt.setString(8, value.patientGuid);
        insertStmt.setString(9, value.artNumber);
        insertStmt.setString(10, value.mflCode);
        insertStmt.setString(11, value.cd4);
        insertStmt.setString(12, value.viralLoad);

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
            insertStmt.setTimestamp(13, dateOfBledTs);
        } else {
            insertStmt.setNull(13, Types.TIMESTAMP);
        }

        // regimenId
        if (value.regimenId != null) {
            insertStmt.setInt(14, value.regimenId);
        } else {
            insertStmt.setNull(14, Types.INTEGER);
        }

        insertStmt.setString(15, value.regimenCode);

        // duration
        if (value.duration != null) {
            insertStmt.setInt(16, value.duration);
        } else {
            insertStmt.setNull(16, Types.INTEGER);
        }

        insertStmt.setString(17, value.medicationId);
        insertStmt.setBigDecimal(18, value.unitQtyPerDose);
        insertStmt.setString(19, value.frequency);
        insertStmt.setString(20, value.unitOfMeasurement);

        insertStmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
    }
}
