package edgelab.retryFreeDB.repo.storage;

import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Postgres implements Storage{
    private static String url = "";
    private static final String user = "user";
    private static final String password = "password";

    private static final long LOCK_THINKING_TIME = 5;
    private static final long OPERATION_THINKING_TIME = 5;



    private String partitionId;

    public Postgres(String addr, String port) {
        url = "jdbc:postgresql://" + addr + ":" + port + "/postgres";
    }


    public Connection connect() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            log.info("Connection created");
        } catch (SQLException e) {
            log.info(e.getMessage());
            throw e;
        }
        return conn;
    }


//    public static void main(String[] args) {
//        Storage storage = new Storage("5430", Server.getLogger("test"));
////        storage.put("khiar", "green");
////        storage.put("apple", "yellow");
////        storage.put("yegear", "white");
//        HashMap<String, String > table = new HashMap<>();
//        table.put("coffee", "black");
//        table.put("tee", "brown");
//        storage.putAll(table);
//        System.out.println(storage.get("khiar"));
//        System.out.println(storage.get("apple"));
//        System.out.println(storage.getAll());
//
//    }

    private String getTable() {
        return "data" + partitionId;
    }

    public String get(String key) {
        String SQL = "SELECT value FROM " + getTable() + " WHERE key = ?";
        String value = null;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next())
                value = rs.getString("value");

        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

        return value;
    }

    public Boolean containsKey(String key) {
        String SQL = "SELECT value FROM " + getTable() +  " WHERE key = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next())
                return true;

        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

        return false;
    }

    public void put(String key, String value) {
        String insertSQL = "INSERT INTO "+ getTable() +" (key, value) " +
                "VALUES (?,?)" +
                "ON CONFLICT (key) DO UPDATE " +
                "    SET value = excluded.value; ";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            pstmt.setString(1, key);
            pstmt.setString(2, value);

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }
    }


    public void remove(String key) {

        String SQL = "DELETE FROM " + getTable() + " WHERE key = ?";;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

    }

    public HashMap<String, String> getAll() {
        String SQL = "SELECT * FROM " + getTable();
        HashMap<String, String> table = new HashMap<>();
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                table.put(rs.getString("key"), rs.getString("value"));
            }

        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }
        return table;
    }


    public void putAll(Map<String, String> table) {
        String insertSQL = "INSERT INTO " + getTable() + " (key, value) " +
                "VALUES (?,?)" +
                "ON CONFLICT (key) DO UPDATE " +
                "    SET value = excluded.value; ";
        String SQL = insertSQL;

        try{
            Connection conn = connect();

            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for(Map.Entry<String, String> entry : table.entrySet()) {
                pstmt.setString(1, entry.getKey());
                pstmt.setString(2, entry.getValue());
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.close();
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }
    }

    public void clear() {
        String SQL = "DELETE FROM " + getTable();

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            log.info(ex.getMessage());
        }

    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    private void update(Connection conn, DBTransaction transaction) {
        log.info("Updating the transaction");
        for (DBData data: transaction.getDataList()) {
            String lockSQL = "SELECT * FROM ? WHERE ? = ? FOR UPDATE";
            try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
                updateStmt.setString(1, data.getTable());
                updateStmt.setString(2, data.getId());
                updateStmt.setInt(3, data.getQuery());
                updateStmt.executeUpdate();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't lock,  {}", ex.getMessage());
                return;
            }
        }
        log.info("Locks on rows acquired");
    }
    public void lockAll(Connection conn, DBTransaction transaction) {
        log.info("Acquiring lock for transaction");
        for (DBData data: transaction.getDataList()) {
            String lockSQL = "SELECT * FROM ? WHERE ? = ? FOR UPDATE";
            try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
                updateStmt.setString(1, data.getTable());
                updateStmt.setString(2, data.getId());
                updateStmt.setInt(3, data.getQuery());
                updateStmt.executeUpdate();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't lock,  {}", ex.getMessage());
                return;
            }
        }
        log.info("Locks on rows acquired");
    }
    public void lock(String tx, Connection conn, DBData data) throws SQLException {
        log.warn("{}, Acquiring lock for data, {}:<{},{}>",tx, data.getTable(), data.getId(), data.getQuery());
        if (!(data instanceof DBInsertData)) {
//        FIXME: Risk of sql injection
            String lockSQL = "SELECT * FROM " + data.getTable() + " WHERE " + data.getId() + " = ? FOR UPDATE";
            try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
                updateStmt.setInt(1, data.getQuery());
                ResultSet rs = updateStmt.executeQuery();
                if (!rs.next()) {
                    log.info("no row with id found!");
                    getAdvisoryLock(conn, data.getTable(), data.getQuery());
                }

                delay(LOCK_THINKING_TIME);
            } catch (SQLException ex) {
                log.info("db error: couldn't lock,  {}", ex.getMessage());
                throw ex;
            }
            log.warn("{}, Locks on rows acquired, {}:<{},{}>",tx, data.getTable(), data.getId(), data.getQuery());
        }
        else {
            getAdvisoryLock(conn, data.getTable(), Integer.parseInt (((DBInsertData) data).getRecordId()));
        }
    }

    private void delay(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void getAdvisoryLock(Connection conn, String tableName, Integer id) throws SQLException {
        String lockSQL = "SELECT pg_advisory_xact_lock('" + tableName + "'::regclass::integer, ?)";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.setInt(1, id);
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.info("db error: couldn't lock,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Advisory lock on {},{} is acquired", tableName, id);
    }


    public void lockTable(Connection conn, DBInsertData data) throws SQLException {
        log.info("Acquiring table lock for data");
//        FIXME: Risk of sql injection
        String lockSQL = "LOCK TABLE "+ data.getTable() +" IN ACCESS EXCLUSIVE";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.executeQuery();
        }
        catch (SQLException ex) {
            log.info("db error: couldn't lock the table,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Locks on table {} acquired", data.getTable());
    }
public void release(Connection conn) throws SQLException { try {
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            log.error("Could not release the locks: {}", e.getMessage());
            throw e;
        }
    }

    public void remove(Connection conn, DBDeleteData data) throws SQLException {

        String SQL = "DELETE FROM "+ data.getTable() +" WHERE " + data.getId() + " = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setInt(1, data.getQuery());
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not remove the data: {}", e.getMessage());
            throw e;
        }
    }

    public void update(Connection conn, DBWriteData data) throws SQLException {
        String SQL = "UPDATE " + data.getTable() + " SET  " + data.getVariable() + " = " + data.getValue() + " WHERE "+ data.getId() +" = ?";
        log.info("update {}:<{}, {}>", data.getTable(), data.getId(), data.getQuery());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setInt(1, data.getQuery());
            pstmt.executeUpdate();
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException e) {
            log.error("Could not write the data: {}", e.getMessage());
            throw e;
        }
    }


    public String get(Connection conn, DBData data) throws SQLException {
        StringBuilder value = new StringBuilder();
        String SQL = "SELECT * FROM "+ data.getTable() +" WHERE "+data.getId()+" = ?";
        log.info("get {}:{}", data.getTable(), data.getId());
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setInt(1, data.getQuery());
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
            delay(OPERATION_THINKING_TIME);
        } catch (SQLException ex) {
            log.error("could not read: {}", ex.getMessage());
            throw ex;
        }

        return value.toString();
    }


    public void insert(Connection conn, DBInsertData data) throws SQLException {
        String SQL = "INSERT INTO " + data.getTable() + " VALUES  (" + data.getRecordId() + "," + data.getNewRecord() + ")";
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

    public Integer lastId(String table) throws SQLException {

        try (Connection conn = connect()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, table.toLowerCase(), null)) {
                if (columns.next()) {
                    String firstColumnName = columns.getString("COLUMN_NAME");
                    String query = "SELECT MAX(\"" + firstColumnName + "\") FROM " + table;
                    try (Statement statement = conn.createStatement();
                         ResultSet resultSet = statement.executeQuery(query)) {
                        if (resultSet.next()) {
                            int nextId = Integer.parseInt(resultSet.getString(1));
                            conn.close();
                            return nextId;
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            log.info(ex.getMessage());
            throw ex;
        }

        return 0;
    }
}



