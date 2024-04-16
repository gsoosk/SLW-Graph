package edgelab.retryFreeDB;

import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.proto.Data;
import edgelab.proto.Empty;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static edgelab.retryFreeDB.RetryFreeDBConfiguration.DELETE_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.INSERT_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.READ_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.WRITE_TYPE;

@Slf4j
public class Client {
    private final RetryFreeDBServerGrpc.RetryFreeDBServerBlockingStub blockingStub;
    private final ManagedChannel channel;

    public Client(String toConnectAddress, int toConnectPort)
    {
        this.channel = ManagedChannelBuilder.forAddress(toConnectAddress, toConnectPort).usePlaintext().build();
        this.blockingStub = RetryFreeDBServerGrpc.newBlockingStub(channel);
    }

    public static void main(String args[])
    {

        Client client = new Client(args[0], Integer.parseInt(args[1]));
        Map<String, HashSet<String>> hotRecords = new HashMap<>();


//          client.buyListing("2", "1");
//          client.buyListingSLW("2", "100000000");
          client.simulateDeadlock();
//        client.addListing("7", "7", 280);
//        client.addListingSLW("7", "7", x280);
    }

    private void simulateDeadlock() {
        String tx1 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx2 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        executor.submit(() -> lock(tx1, "Players", "PId", "10"));
        delay();
        executor.submit(() -> lock(tx2, "Players", "PId", "11"));
        delay();
        executor.submit(() -> lock(tx1, "Players", "PId", "11"));
        delay();
        executor.submit(() -> lock(tx2, "Players", "PId", "10"));// DEADLOCK


        while (executor.getActiveCount() != 0) {
            delay();
        }


        commit(tx1);
        commit(tx2);


    }

    private static void delay() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


//    remoteCalls : 4
    public String addListingDeadlockDetect(String PId, String IId, double price) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();
//
//            Map<String, String> listing = read(transactionId, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(transactionId);
//                return;
//            }

            if (!lock(transactionId, "Items", "IId", IId).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> item = read(transactionId, "Items", "IId", IId);
//             Check the owner
            if (Integer.parseInt( item.get("iowner")) != Integer.parseInt(PId)) {
                log.info("item has a different owner! {}<>{}", item.get("iowner"), PId);
                commit(transactionId);
                return null;
            }

            if (!lock(transactionId, "Players", "PId", PId).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
            if (player.isEmpty()) {
                log.info("player does not exists!");
                commit(transactionId);
                return null;
            }

            String listingRecordId = insertLock(transactionId, "Listings");
            insert(transactionId, "Listings",  IId + "," + price, listingRecordId);
            commit(transactionId);
            return listingRecordId;
        }

        return null;
    }

    private void rollback(String transactionId) {
        Result rollbackRes = blockingStub.rollBackTransaction(TransactionId.newBuilder().setId(transactionId).build());
        locks.remove(transactionId);
    }

    //    Remote calls: 4
    public String addListingSLW(String PId, String IId, double price) {
        log.info("add listing <PID:{}, IID:{}>", PId, IId);
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();

            String listingRecordId = insertLock(transactionId, "Listings");

            lock(transactionId, "Items", "IId", IId);
            Map<String, String> item = read(transactionId, "Items", "IId", IId);
//             Check the owner
            if (Integer.parseInt( item.get("iowner")) != Integer.parseInt(PId)) {
                log.error("item has a different owner! {}<>{}", item.get("iowner"), PId);
                commit(transactionId);
                return null;
            }

            lock(transactionId, "Players", "PId", PId);
            Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
            if (player.isEmpty()) {
                log.error("player does not exists!");
                commit(transactionId);
                return null;
            }

            insert(transactionId, "Listings",  IId + "," + price, listingRecordId);
            commit(transactionId);
            return listingRecordId;
        }
        return null;
    }

    public String buyListingDeadlockDetect(String PId, String LId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();

        //            R from L where Lid
            if (!lock(transactionId, "Listings", "LId", LId).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> listing = read(transactionId, "Listings", "LId", LId);
//            Check player exists
            if (listing.isEmpty()) {
                log.info("listing does not exists!");
                commit(transactionId);
                return null;
            }

            //            Read from P where pid
            if(!lock(transactionId, "Players", "PId", PId).getStatus()) {
                rollback(transactionId);
                return null;
            }
//            TODO: WE MIGHT NEED TO ROLLBACK
            Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
            if (player.isEmpty()) {
                log.info("player does not exists!");
                commit(transactionId);
                return null;
            }

            //            Check players cash for listing
            if (Double.parseDouble( player.get("pcash")) < Double.parseDouble(listing.get("lprice"))){
                log.info("player does not have enough cash");
                commit(transactionId);
                return null;
            }


//            Read from I where LIID
            if (!lock(transactionId, "Items", "IId", listing.get("liid")).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> item = read(transactionId, "Items", "IId", listing.get("liid"));
            //            Check item exists
            if (item.isEmpty()) {
                log.info("item does not exists!");
                commit(transactionId);
                return null;
            }


//            Delete from L where LID
            delete(transactionId, "Listings", "LId", LId);


//            R from P where ppid
            String PPId =  item.get("iowner");
            if (!lock(transactionId, "Players", "PId", PPId).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> prevOwner = read(transactionId, "Players", "PId", PPId);
            //            Check prevOwner exists
            if (prevOwner.isEmpty()) {
                log.info("previous owner does not exists!");
                commit(transactionId);
                return null;
            }

//          W into I where IId = Liid SET Iowner = pid
            write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);

//           W into P where Pid SET pCash = new Cash
            String newCash = Double.toString(Double.parseDouble( player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//           W into P where Pid SET pCash = new Cash
            String prevOwnerNewCash = Double.toString(Double.parseDouble( prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);

//            Unlock
            commit(transactionId);
            return listing.get("liid");
        }
        return null;
    }

    public String buyListingSLW(String PId, String LId) {
        log.info("buy listing <PID:{}, LID:{}>", PId, LId);
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();
            //            R from L where Lid
            lock(transactionId, "Listings", "LId", LId);
            Map<String, String> listing = read(transactionId, "Listings", "LId", LId);
//            Check player exists
            if (listing.isEmpty()) {
                log.error("listing does not exists!");
                commit(transactionId);
                return null;
            }

//            Read from I where LIID
            lock(transactionId, "Items", "IId",  listing.get("liid"));
            Map<String, String> item = read(transactionId, "Items", "IId", listing.get("liid"));
            //            Check item exists
            if (item.isEmpty()) {
                log.error("item does not exists!");
                commit(transactionId);
                return null;
            }

//            Lock Players where pid, ppid
            String PPId =  item.get("iowner");
            if (Integer.parseInt(PPId) > Integer.parseInt(PId)) {
                lock(transactionId, "Players", "PId", PId);
                lock(transactionId, "Players", "PId", PPId);
            }
            else {
                lock(transactionId, "Players", "PId", PPId);
                lock(transactionId, "Players", "PId", PId);
            }

            //            Read from P where pid
            Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
            if (player.isEmpty()) {
                log.error("player does not exists!");
                commit(transactionId);
                return null;
            }

            //            Check players cash for listing
            if (Double.parseDouble( player.get("pcash")) < Double.parseDouble(listing.get("lprice"))){
                log.error("player does not have enough cash");
                commit(transactionId);
                return null;
            }


            Map<String, String> prevOwner = read(transactionId, "Players", "PId", PPId);
            //            Check prevOwner exists
            if (prevOwner.isEmpty()) {
                log.error("previous owner does not exists!");
                commit(transactionId);
                return null;
            }




//            Delete from L where LID
            delete(transactionId, "Listings", "LId", LId);

//          W into I where IId = Liid SET Iowner = pid
            write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);

//           W into P where Pid SET pCash = new Cash
            String newCash = Double.toString(Double.parseDouble( player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//           W into P where Pid SET pCash = new Cash
            String prevOwnerNewCash = Double.toString(Double.parseDouble( prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);

//            Unlock
            commit(transactionId);
            return listing.get("liid");
        }
        return null;
    }

    private Map<String, Set<String>> locks = new HashMap<>();

    private Result performRemoteOperation(Data data) {
        boolean lockBeforeOperation = true;
        if (locks.containsKey(data.getTransactionId())) {
            if (locks.get(data.getTransactionId()).contains(data.getKey())) {
                lockBeforeOperation = false;
            }
            else {
                locks.get(data.getTransactionId()).add(data.getKey());
            }
        }
        else {
            locks.put(data.getTransactionId(), new HashSet<>());
        }
        Result result;
        if (lockBeforeOperation)
            result = blockingStub.lockAndUpdate(data);
        else
            result = blockingStub.update(data);
        return result;
    }
    private void commit(String transactionId) {
        Result commitResult = blockingStub.commitTransaction(TransactionId.newBuilder().setId(transactionId).build());
        locks.remove(transactionId);
    }

    private void write(String transactionId, String tableName, String key, String value, String newKey, String newValue) {
        Data writeData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .setValue(newKey + "," + newValue)
                .build();
        Result writeResult = performRemoteOperation(writeData);
        log.info("Write to {} status: {}", tableName, writeResult.getStatus());
    }

    private void delete(String transactionId, String tableName, String key, String value) {
        Data deleteData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(DELETE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result deleteResult = performRemoteOperation(deleteData);
        log.info("delete from {} status: {}", tableName, deleteResult.getStatus());
    }

    private Map<String, String> read(String transactionId, String tableName, String key, String value)  {
        Data readData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result readResult = performRemoteOperation(readData);
        log.info("read from {} status : {}", tableName, readResult.getStatus());
        return convertStringToMap(readResult.getMessage());
    }


    private void insert(String transactionId, String tableName, String newRecord, String recordId) {
        Data insertData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .setRecordId(recordId)
                .build();
        Result result = blockingStub.update(insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
    }

    private void insert(String transactionId, String tableName, String newRecord) {
        Data insertData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .build();
        Result result = blockingStub.lockAndUpdate(insertData);
        log.info("insert data with into {} status : {}", tableName, result.getStatus());
    }

    private Result lock(String transactionId, String tableName, String key, String value) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("{}, lock on {},{}:{} status: {} - message: {}",transactionId, tableName, key, value, lockResult.getStatus(), lockResult.getMessage());
        if (locks.containsKey(transactionId)) {
            locks.get(transactionId).add(lockData.getKey());
        }
        else {
            locks.put(transactionId, new HashSet<>());
            locks.get(transactionId).add(lockData.getKey());
        }
        return lockResult;
    }

    private String insertLock(String transactionId, String tableName) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("lock on {} for insert status: {}", tableName, lockResult.getStatus());
        return lockResult.getMessage();
    }

    public static Map<String, String> convertStringToMap(String str) {
        Map<String, String> map = new HashMap<>();

        // Split the string into key-value pairs
        String[] pairs = str.split(",");

        for (String pair : pairs) {
            // Split each pair to get key and value
            String[] keyValue = pair.split(":", 2);
            if (keyValue.length == 2) { // Check if there's a key and a value
                map.put(keyValue[0], keyValue[1]);
            }
        }

        return map;
    }
}


