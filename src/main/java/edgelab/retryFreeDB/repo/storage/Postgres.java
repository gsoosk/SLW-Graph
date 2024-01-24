package edgelab.retryFreeDB.repo.storage;

import edgelab.proto.Transaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Postgres implements Storage{
    private static String url = "";
    private static final String user = "user";
    private static final String password = "password";



    private String partitionId;

    public Postgres(String port) {
        url = "jdbc:postgresql://localhost:" + port + "/postgres";
    }


    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            log.info(e.getMessage());
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
                updateStmt.setString(3, data.getQuery());
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
                updateStmt.setString(3, data.getQuery());
                updateStmt.executeUpdate();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't lock,  {}", ex.getMessage());
                return;
            }
        }
        log.info("Locks on rows acquired");
    }
    public void lock(Connection conn, DBData data) throws SQLException {
        log.info("Acquiring lock for data");
        String lockSQL = "SELECT * FROM ? WHERE ? = ? FOR UPDATE";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.setString(1, data.getTable());
            updateStmt.setString(2, data.getId());
            updateStmt.setString(3, data.getQuery());
            updateStmt.executeUpdate();
        }
        catch (SQLException ex) {
            log.info("db error: couldn't lock,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Locks on rows acquired");
    }

    public void release(Connection conn) throws SQLException {
        try {
            conn.commit();
        } catch (SQLException e) {
            log.error("Could not release the locks: {}", e.getMessage());
            throw e;
        }
    }

    public void remove(Connection conn, DBDeleteData data) throws SQLException {

        String SQL = "DELETE FROM ? WHERE ? = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setString(1, data.getTable());
            pstmt.setString(2, data.getId());
            pstmt.setString(3, data.getQuery());
            pstmt.executeUpdate();

        } catch (SQLException e) {
            log.error("Could not remove the data: {}", e.getMessage());
            throw e;
        }
    }

    public void update(Connection conn, DBWriteData data) throws SQLException {
        String SQL = "UPDATE ? SET  ? = ? WHERE ? = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setString(1, data.getTable());
            pstmt.setString(2, data.getId());
            pstmt.setString(3, data.getQuery());
            pstmt.setString(4, data.getVariable());
            pstmt.setString(5, data.getValue());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Could not remove the data: {}", e.getMessage());
            throw e;
        }
    }


    public String get(Connection conn, DBData data) throws SQLException {
        String value = "";
        String SQL = "SELECT value FROM ? WHERE ? = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(SQL);
            pstmt.setString(1, data.getTable());
            pstmt.setString(2, data.getId());
            pstmt.setString(3, data.getQuery());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next())
                value = rs.getString("value");

        } catch (SQLException ex) {
            log.info(ex.getMessage());
            throw ex;
        }

        return value;
    }




}



