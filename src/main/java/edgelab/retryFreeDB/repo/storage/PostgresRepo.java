package edgelab.retryFreeDB.repo.storage;

import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

@Slf4j
public class PostgresRepo implements KeyValueRepository {
    private void delay(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insert(DBTransaction tx, DBInsertData data) throws SQLException {
        Connection conn = tx.getConnection();
        String SQL = data.getNewRecord().isEmpty() ? "INSERT INTO " + data.getTable() + " VALUES  (" + data.getRecordId() + ")"
                : "INSERT INTO " + data.getTable() + " VALUES  (" + data.getRecordId() + "," + data.getNewRecord() + ")";
        log.info("insert {}:{}", data.getTable(), data.getRecordId());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not insert the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void remove(DBTransaction tx, DBDeleteData data) throws Exception {
        Connection conn = tx.getConnection();

        String SQL = "DELETE FROM "+ data.getTable() +" WHERE ";
        for (int i = 0 ; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }

        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not remove the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void write(DBTransaction tx, DBWriteData data) throws Exception {
        Connection conn = tx.getConnection();

        String SQL = "UPDATE " + data.getTable() + " SET  " + data.getVariable() + " = " + data.getValue() + " WHERE ";
        for (int i = 0 ; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }

        log.info("update {}:<{}, {}>", data.getTable(), data.getIds(), data.getQueries());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not write the data: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public String read(DBTransaction tx, DBData data) throws Exception {
        Connection conn = tx.getConnection();
        StringBuilder value = new StringBuilder();
        String SQL = "SELECT * FROM "+ data.getTable() +" WHERE ";
        for (int i = 0; i < data.getIds().size(); i++) {
            if (i != 0)
                SQL += "AND ";
            SQL += data.getIds().get(i) + " = ? ";
        }
        log.info("get {}:{}", data.getTable(), data.getIds());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for (int i = 1; i <= data.getQueries().size() ; i++)
                pstmt.setInt(i, data.getQueries().get(i-1));
            ResultSet rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    Object columnValue = rs.getObject(i);
                    value.append(metaData.getColumnName(i)).append(":");
                    if (columnValue instanceof String)
                        value.append("'").append(columnValue).append("'");
                    else
                        value.append(columnValue);
                    if (i != columnCount)
                        value.append(",");
                }
            }
        } catch (SQLException ex) {
            log.error("could not read: {}", ex.getMessage());
            throw ex;
        }
        return value.toString();
    }
}
