package edgelab.retryFreeDB.repo.storage;

import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Postgres implements Storage{
    public static final String DEADLOCK_ERROR = "40P01";
    private static String url = "";
    private static final String user = "user";
    private static final String password = "password";

    private static final long LOCK_THINKING_TIME = 0;
    private static final long OPERATION_THINKING_TIME = 5;

    private String partitionId;

    private final ConcurrentHashMap<String, Lock> resourceLocks = new ConcurrentHashMap<>();
    private Lock getLock(String resource) {
        return resourceLocks.computeIfAbsent(resource, k -> new ReentrantLock());
    }


//    Wound-Wait lists : Resource: List <Owners|Waiters|Retired(for bamboo)>
//    Assumption: All the locks are exclusive
    private ConcurrentHashMap<String, List<String>> owners = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> waiters = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> retired = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Set<String>> txResources = new ConcurrentHashMap<>();

    public Postgres(String addr, String port) {
        url = "jdbc:postgresql://" + addr + ":" + port + "/postgres";
    }

    private void setDeadlockDetectionTimeout(Connection conn, String timeout) throws SQLException {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("SET deadlock_timeout = '" + timeout + "'");
        } catch (SQLException e) {
            log.error("Could not initialize deadlock detection");
            throw e;
        }

    }


    public Connection connect() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
            setDeadlockDetectionTimeout(conn, "1s");
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
    private final Map<String, DBLock> locks = new HashMap<>();
    private final Set<String> abortedTransactions = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Set<String>> transactionResources = new ConcurrentHashMap<>();
    public void lock(String tx, Connection conn, Set<String> toBeAborted, DBData data) throws Exception {
        String resource = (data instanceof DBInsertData) ? data.getTable() +  ","  + ((DBInsertData) data).getRecordId() : data.getTable() +  ","  + data.getQuery();
        DBLock lock;
        log.info("{}, try to lock {}",tx, resource);
        synchronized (locks) {
            lock = locks.getOrDefault(resource, new DBLock(resource));
            locks.put(resource, lock);
        }
        LockType lockType = LockType.WRITE; // TODO: Read lock

        synchronized (lock) {
            log.info("START____________________________{},{}_____________________", tx, lock.getResource());
            if (!lock.canGrant(tx, lockType))
                handleConflict(tx, lock, toBeAborted);
            log.info("{}, {}", tx, lock);
            while (!lock.canGrant(tx, lockType)) {
                try {
                    log.info("{}: waiting for lock on {}", tx, resource);
                    lock.wait();
                    if (abortedTransactions.contains(tx)) {
                        log.error("Transaction is aborted. Could not lock");
                        throw new Exception("Transaction aborted. can not lock");
                    }
                    log.info("{}: wakes up to check the lock {}", tx, resource);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            lock.grant(tx, lockType);
            addNewResourceForTransaction(tx, resource);
            log.info("{}: Lock granted on {} for {}", tx, resource, lockType);
            log.info("END____________________________{},{}_____________________", tx, lock.getResource());
        }

        delay(LOCK_THINKING_TIME);
    }

    private void addNewResourceForTransaction(String tx, String resource) {
        transactionResources.putIfAbsent(tx, new HashSet<>());
        transactionResources.get(tx).add(resource);
    }

    private void handleConflict(String tx, DBLock lock, Set<String> toBeAborted) {
        synchronized (lock) {
//            TODO: Wound Wait;
            log.info("previous holding transactons: {}", lock.getHoldingTransactions());
            for (String t : lock.getHoldingTransactions()) {
                if (Long.parseLong(t) > Long.parseLong(tx)) {
//                    abort transaction t
                    toBeAborted.add(t);
                    abortedTransactions.add(t);
                    releaseLock(t, lock);

                    lock.addPending(tx);
                    addNewResourceForTransaction(tx, lock.getResource());
//                    unlockAll(t);
//                    lock.release(t);
//                    transactionResources.get(t).remove(lock.getResource());

                    log.info("{}: Wound-wait: {} aborted by {}",tx, t, tx);
                    log.info("after holding transactons: {}", lock.getHoldingTransactions());
                    return;
                }
            }
            log.info("{}: Transaction waits for lock on {}", tx, lock.getResource());
            lock.addPending(tx);
            addNewResourceForTransaction(tx, lock.getResource());
        }
    }

//
//
//    public void lock(String tx, Connection conn, Set<String> toBeAborted, DBData data) throws SQLException {
//
//        log.warn("{}, Acquiring lock for data, {}:<{},{}>",tx, data.getTable(), data.getId(), data.getQuery());
//
//        String s = (data instanceof DBInsertData) ? data.getTable() +  ","  + ((DBInsertData) data).getRecordId() : data.getTable() +  ","  + data.getQuery();
//        getLock(s + "+1").lock();
//        bambooWound(tx, s, toBeAborted);
////      Assumption: Only lock based on the primary key
//
//        getLock(s + "+1").unlock();
//        getLock(s + "+2").lock();
//        if (!(data instanceof DBInsertData)) {
////        FIXME: Risk of sql injection
////            String lockSQL = "SELECT * FROM " + data.getTable() + " WHERE " + data.getId() + " = ? FOR UPDATE";
////            try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL))
//            try
//            {
////                updateStmt.setInt(1, data.getQuery());
////                ResultSet rs = updateStmt.executeQuery();
////                if (!rs.next()) {
////                    log.info("no row with id found!");
//                getAdvisoryLock(conn, data.getTable(), data.getQuery());
////                }
//
//                delay(LOCK_THINKING_TIME);
//            } catch (SQLException ex) {
//
//                log.error("{}, db error: couldn't lock,  {}:CODE:{}", tx, ex.getMessage(), ex.getSQLState());
//                throw ex;
//            }
//            log.warn("{}, Locks on rows acquired, {}:<{},{}>", tx, data.getTable(), data.getId(), data.getQuery());
//        }
//        else {
//            getAdvisoryLock(conn, data.getTable(), Integer.parseInt (((DBInsertData) data).getRecordId()));
//        }
//
//        bambooPromoteWaiter(tx, s);
//        getLock(s + "+2").unlock();
//    }


    private void bambooPromoteWaiter(String tx, String s) {

        this.owners.get(s).add(tx);
        this.waiters.get(s).remove(tx);
        this.txResources.get(tx).add(s);

        log.info("Bamboo: {} is now the owner of {}", tx, s);
    }

    private void bambooWound(String tx, String resource, Set<String> toBeAborted) {
        this.txResources.putIfAbsent(tx, ConcurrentHashMap.newKeySet());
        this.owners.putIfAbsent(resource, Collections.synchronizedList(new ArrayList<>()));
        this.waiters.putIfAbsent(resource, Collections.synchronizedList(new ArrayList<>()));

        if (!this.owners.get(resource).isEmpty()) {
            if (Long.parseLong(tx) < Long.parseLong(this.owners.get(resource).get(0))) {
                log.info("Bamboo: wound younger transaction <{} wounds {}>", tx, this.owners.get(resource).get(0));
//                wound the younger: abort ownersList.get(0)
                toBeAborted.add(this.owners.get(resource).get(0));
            }
        }

        this.waiters.get(resource).add(tx);


    }

    private void delay(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void getAdvisoryLock(Connection conn, String tableName, Integer id) throws SQLException {
        String lockSQL = "SELECT pg_advisory_lock('" + tableName + "'::regclass::integer, ?)";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.setInt(1, id);
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.info("db error: couldn't lock,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Advisory lock on {},{} is acquired", tableName, id);
    }

    private  void unlockAllAdvisoryLocks(String tx, Connection conn) throws SQLException {
        bambooReleaseAllLocks(tx);
        String lockSQL = "SELECT pg_advisory_unlock_all()";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.info("db error: couldn't unlock all,  {}", ex.getMessage());
            throw ex;
        }
        log.info("All Advisory locks unlocked");

    }

    private void bambooReleaseAllLocks(String tx) {
        for (String s : this.txResources.get(tx)) {
            bambooReleaseLock(tx, s);
        }
        txResources.remove(tx);
        log.info("Bamboo: all resources for transaction {} was released", tx);
    }

    public void unlock(String tx, ConcurrentHashMap<String, Connection> transactions, DBData data) throws SQLException {
        String resource = (data instanceof DBInsertData) ? data.getTable() +  ","  + ((DBInsertData) data).getRecordId() : data.getTable() +  ","  + data.getQuery();
        releaseLock(tx, resource);
    }

    private void releaseLock(String tx, String resource) {
        DBLock lock;
        synchronized (locks) {
            lock = locks.get(resource);
        }

        releaseLock(tx, lock);
    }

    private void releaseLock(String tx, DBLock lock) {
        if (lock != null) {
            synchronized (lock) {
                lock.release(tx);
                transactionResources.get(tx).remove(lock.getResource());
                log.info("{}: Lock released  on {}", tx, lock.getResource());
                lock.notifyAll(); // Notify all waiting threads
            }
        }
        else {
            log.error("{}, Lock was not held to be released!", tx);
        }
    }

    public void unlockAll(String tx) {
        synchronized (locks) {
            for (String resource : new HashSet<>(transactionResources.get(tx))) {
                releaseLock(tx, resource);
            }
            transactionResources.get(tx).clear();
        }
    }

//
//    public void unlock(String tx, ConcurrentHashMap<String, Connection> transactions, DBData data) throws SQLException {
//        bambooReleaseLock(tx, data.getTable() + "," + data.getQuery());
//        unlockAdvisory(transactions.get(tx), data.getTable(), data.getQuery());
//    }



    private void bambooReleaseLock(String tx, String s) {
        if (owners.get(s).isEmpty())
            log.error("Lock was not held to be released!");
        else {
            owners.get(s).remove(0);
            txResources.get(tx).remove(s);
            log.info("Bamboo: resource {} for transaction {} was released: owners: {}", s, tx, owners.get(s));
        }
    }

    private static void unlockAdvisory(Connection conn, String tableName, Integer id) throws SQLException {
        String lockSQL = "SELECT pg_advisory_unlock('" + tableName + "'::regclass::integer, ?)";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.setInt(1, id);
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.error("db error: couldn't unlock,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Advisory lock unlocked {},{}", tableName, id);
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
    public void release(String tx, Connection conn) throws SQLException {
        try {
            conn.commit();
//            unlockAllAdvisoryLocks(tx, conn);
            unlockAll(tx);
            conn.close();
        } catch (SQLException e) {
            log.error("Could not release the locks: {}", e.getMessage());
            throw e;
        }
    }


    public void rollback(String tx, Connection conn) throws SQLException {
        try {
            ((PGConnection) conn).cancelQuery();

            conn.rollback();
//            unlockAllAdvisoryLocks(tx, conn);
            unlockAll(tx);
            conn.close();
        } catch (SQLException e) {
            log.error("Could not rollback and release the locks: {}", e.getMessage());
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

    public boolean isValid(Connection b) {
        try {
            return b.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }
}



