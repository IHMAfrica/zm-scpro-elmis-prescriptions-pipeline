package zm.gov.moh.hie.scp.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import zm.gov.moh.hie.scp.model.PrescriptionRecord;
import zm.gov.moh.hie.scp.util.DateTimeUtil;
import java.sql.*;
import java.time.LocalDateTime;

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
                "uuid, hmis_code, drug_count, regimen_count, date, \"time\") " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        insertStmt = connection.prepareStatement(insertQuery);
    }

    @Override
    public void invoke(PrescriptionRecord value, Context context) throws Exception {
        insertStmt.setString(1, value.messageId);
        insertStmt.setString(2, value.hmisCode);
        insertStmt.setInt(3, value.drugCount);
        insertStmt.setInt(4, value.regimenCount);

        LocalDateTime timestamp = LocalDateTime.parse(value.mshTimestamp, DateTimeUtil.TIMESTAMP_FORMATTER);
        Timestamp ts = Timestamp.valueOf(timestamp);
        insertStmt.setDate(5, new java.sql.Date(ts.getTime()));
        insertStmt.setTime(6, new java.sql.Time(ts.getTime()));

        insertStmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) insertStmt.close();
        if (connection != null) connection.close();
    }
}
