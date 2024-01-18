package edgelab.retryFreeDB.repo.storage;

import edgelab.proto.Transaction;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.loging.Logger;

@Slf4j
public class Postgres implements Storage{
    private static String url = "";
    private static final String user = "user";
    private static final String password = "password";



    private String partitionId;

    public Postgres(String port, Logger log) {
        url = "jdbc:postgresql://localhost:" + port + "/postgres";
    }


    private Connection connect() {
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

    @Override
    public void twoPL(Transaction transaction) {


        try (Connection conn = connect()) {





        } catch (SQLException ex) {
            log.info("Couldn't connect to db,  {}", ex.getMessage());
        } 
    }
}



