package zm.gov.moh.hie.scp.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;

import java.sql.*;

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
        ensureTableExists(connection, table);
        String sql = "INSERT INTO " + table + " (message_id, msh_timestamp, patient_uuid, art_number, cd4, viral_load, date_of_bled, regimen_id, clinician_name, prescription_uuid, prescription_date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        insertStmt = connection.prepareStatement(sql);
    }

    private void ensureTableExists(Connection conn, String table) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "id SERIAL PRIMARY KEY, " +
                "message_id VARCHAR(255), " +
                "msh_timestamp VARCHAR(255), " +
                "patient_uuid VARCHAR(255), " +
                "art_number VARCHAR(255), " +
                "cd4 VARCHAR(255), " +
                "viral_load VARCHAR(255), " +
                "date_of_bled VARCHAR(255), " +
                "regimen_id INTEGER, " +
                "clinician_name VARCHAR(255), " +
                "prescription_uuid VARCHAR(255), " +
                "prescription_date VARCHAR(255)" +
                ")";
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
        }
    }

    @Override
    public void invoke(PrescriptionRecord value, Context context) throws Exception {
        insertStmt.setString(1, value.messageId);
        insertStmt.setString(2, value.mshTimestamp);
        insertStmt.setString(3, value.patientUuid);
        insertStmt.setString(4, value.artNumber);
        insertStmt.setString(5, value.cd4);
        insertStmt.setString(6, value.viralLoad);
        insertStmt.setString(7, value.dateOfBled);
        if (value.regimenId == null) {
            insertStmt.setNull(8, Types.INTEGER);
        } else {
            insertStmt.setInt(8, value.regimenId);
        }
        insertStmt.setString(9, value.clinicianName);
        insertStmt.setString(10, value.prescriptionUuid);
        insertStmt.setString(11, value.prescriptionDate);
        insertStmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
    }
}
