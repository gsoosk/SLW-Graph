package edgelab.retryFreeDB;

import edgelab.proto.Config;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.proto.Data;
import edgelab.proto.Empty;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private Map<String, Set<String>> locks = new ConcurrentHashMap<>();

    private final String mode;

    public Client(String toConnectAddress, int toConnectPort, String mode)
    {
        this.channel = ManagedChannelBuilder.forAddress(toConnectAddress, toConnectPort).usePlaintext().build();
        this.blockingStub = RetryFreeDBServerGrpc.newBlockingStub(channel);
        this.mode = mode;
    }

    public static void main(String args[])
    {

        Client client = new Client(args[0], Integer.parseInt(args[1]), "slw");
        Map<String, HashSet<String>> hotRecords = new HashMap<>();


//          client.buyListing("2", "1");
//        client.addListingSLW("1", "3", 1);
//          client.buyListingSLW("2", "1000");
//          client.simulateDeadlock();
//          client.test();
//        client.addListing("7", "7", 280);
//        client.readItem(List.of("10", "11", "12"));

        client.TPCC_payment("1", "2", 10.0f, "1", "2", "1");
    }

    private void test() {
        String tx1 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx2 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx3 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx4 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx5 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx6 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        executor.submit(()->  {
//            lock(tx3, "Players", "PId", "10", WRITE_TYPE);
//            retireLock(tx3, "Players", "PId", "10");

            lock(tx5, "Players", "PId", "10", WRITE_TYPE);
            retireLock(tx5, "Players", "PId", "10");

            lock(tx6, "Players", "PId", "10", WRITE_TYPE);
            retireLock(tx6, "Players", "PId", "10");


            lock(tx4, "Players", "PId", "10", WRITE_TYPE);


        });
        delay();

        executor.submit(() -> {
            waitForCommit(tx5);
            commit(tx5);
        });

        executor.submit(() -> {
            waitForCommit(tx4);
            commit(tx4);
        });

        executor.submit(() -> {
            waitForCommit(tx3);
            commit(tx3);
        });

        executor.submit(() -> {
            waitForCommit(tx6);
            commit(tx6);
        });
//        executor.submit(()->  lock(tx2, "Players", "PId", "11", WRITE_TYPE));
//        delay();
//
//        executor.submit(()->  {
//            lock(tx2, "Players", "PId", "10", READ_TYPE);
//            log.info("{} now can access 10", tx2);
//            log.info("{} value of player 10: {}", tx2, read(tx2, "Players", "PId", "10"));
//        });
//        delay();
//
//        executor.submit(()->  {
//            lock(tx1, "Players", "PId", "11", READ_TYPE);
//            log.info("{} now can access 11", tx1);
////            lock(tx1, )
//
//        });
//        delay();

//        executor.submit(()-> rollback(tx1));
//        commit(tx2);
//        commit(tx1);

    }

    public boolean TPCC_payment(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId) {
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String tx = initResult.getMessage();

            if (!lock(tx, "warehouse", "w_id", warehouseId, WRITE_TYPE).getStatus()) {
                rollback(tx);
                return false;
            }

//            Get warehouse
            Map<String, String> warehouse = read(tx, "warehouse", "w_id", warehouseId);
            if (warehouse.isEmpty()) {
                rollback(tx);
                return false;
            }


//            update warehouse
            float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd"))  + paymentAmount ;
            if (!write(tx, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance))) {
                rollback(tx);
                return false;
            }


            if (!lock(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE).getStatus()) {
                rollback(tx);
                return false;
            }

//          get district
            Map<String, String> district = read(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId);
            if (district.isEmpty()) {
                rollback(tx);
                return false;
            }

//            Update District
            float newDistrictBalance = Float.parseFloat(district.get("d_ytd"))  + paymentAmount;
            if (!write(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance))) {
                rollback(tx);
                return false;
            }

//            Get customer
            String customerIdKey = "c_w_id,c_d_id,c_id";
            String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId ;
            if (!lock(tx, "customer", customerIdKey, customerIdValue, WRITE_TYPE).getStatus()) {
                rollback(tx);
                return false;
            }


            Map<String, String> customer = read(tx, "customer", customerIdKey, customerIdValue);
            if (customer.isEmpty()) {
                rollback(tx);
                return false;
            }

            float c_balance = Float.parseFloat(customer.get("c_balance")) - paymentAmount;
            float c_ytd_payment = Float.parseFloat(customer.get("c_ytd_payment")) + paymentAmount;
            int c_payment_cnt = Integer.parseInt(customer.get("c_payment_cnt")) + 1;
//            Update customer
            if(!write(tx, "customer", customerIdKey, customerIdValue, "c_balance", String.valueOf(c_balance)) |
                    !write(tx, "customer", customerIdKey, customerIdValue, "c_ytd_payment", String.valueOf(c_ytd_payment)) |
                    !write(tx, "customer", customerIdKey, customerIdValue, "c_payment_cnt", String.valueOf(c_payment_cnt))) {
                rollback(tx);
                return false;
            }
            if (customer.get("c_credit").equals("BC")) {
                String c_data = customerId + " " + customerDistrictId + " " + customerWarehouseId + " " + districtId + " " + warehouseId + " " + paymentAmount + " | " + customer.get("c_data");
                if (c_data.length() > 500) {
                    c_data = c_data.substring(0, 500);
                }

                if (!write(tx, "customer", customerIdKey, customerIdValue, "c_data", c_data)) {
                    rollback(tx);
                    return false;
                }
            }

//            insert history
            String historyId = customerId + "," + customerDistrictId + "," + customerWarehouseId + "," +  districtId + "," + warehouseId;

            if (!insertLock(tx,"history", historyId).getStatus()) {
                rollback(tx);
                return false;
            }


            String h_data = "'" + warehouseId + ":" + districtId + "'";

            if (!insert(tx, "history", "'" + new Timestamp(System.currentTimeMillis()) + "'" + "," +
                    paymentAmount + "," +
                    h_data, historyId)) {
                rollback(tx);
                return false;
            }


            commit(tx);
            return true;
        }

        return false;
    }

    public void setServerConfig(Map<String, String> config) {
        Result result = blockingStub.setConfig(Config.newBuilder().putAllValues(config).build());
        if (!result.getStatus())
            throw new RuntimeException("Could not set the server config!");

    }

    public boolean readItem(List<String> items) {
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String tx = initResult.getMessage();
            if (mode.equals("bamboo") || mode.equals("ww")) {
                if (readItemBamboo(items, tx)) return false;
            }
            else {
                if (readItemSLW(items, tx)) return false;
            }
            return commit(tx).getStatus();

        }
        return false;
    }

    private boolean readItemSLW(List<String> items, String tx) {
//        Collections.sort(items);
        return readItems(items, tx);
    }

    private boolean readItems(List<String> items, String tx) {
        for (String IId:
                items) {
            if (!lock(tx, "Items", "IId", IId, READ_TYPE).getStatus()) {
                rollback(tx);
                return true;
            }

            Map<String, String> item = read(tx, "Items", "IId", IId);
            if (item.isEmpty()) {
                rollback(tx);
                return true;
            }

        }
        return false;
    }

    private boolean readItemBamboo(List<String> items, String tx) {
        if (readItems(items, tx)) return true;
        if (!waitForCommit(tx).getStatus()) {
            rollback(tx);
            return true;
        }
        return false;
    }

    private void simulateDeadlock() {
        String tx1 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();
        String tx2 = blockingStub.beginTransaction(Empty.newBuilder().build()).getMessage();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        executor.submit(() -> lock(tx1, "Players", "PId", "10"));
        delay();
        executor.submit(() -> lock(tx2, "Players", "PId", "11"));
        delay();
        executor.submit(() -> { if (!lock(tx1, "Players", "PId", "11").getStatus())
                                    rollback(tx1);
        });
        delay();
        executor.submit(() -> {

            if (!lock(tx2, "Players", "PId", "10").getStatus()) {
                rollback(tx2);
            };
        });// DEADLOCK


        while (executor.getActiveCount() != 0) {
            delay();
        }


        commit(tx1);
        commit(tx2);


    }

    private static void delay() {
        delay(100);
    }

    private static void delay(long mili) {
        try {
            Thread.sleep(mili);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


//    remoteCalls : 4
    public String addListing(String PId, String IId, double price) {
        return switch (mode) {
            case "slw" -> addListingSLW(PId, IId, price);
            case "bamboo" -> addListingBamboo(PId, IId, price);
            case "ww" -> addListingWW(PId, IId, price);
            default -> null;
        };
    }

    public String addListingWW(String PId, String IId, double price) {

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

            if (!lock(transactionId, "Items", "IId", IId, READ_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> item = read(transactionId, "Items", "IId", IId);
            if (item.isEmpty()) {
                log.info("Item does not exists!");
                commit(transactionId);
                return null;
            }
//             Check the owner
            if (Integer.parseInt( item.get("iowner")) != Integer.parseInt(PId)) {
                log.info("item has a different owner! {}<>{}", item.get("iowner"), PId);
                commit(transactionId);
                return null;
            }

            if (!lock(transactionId, "Players", "PId", PId, READ_TYPE).getStatus()) {
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

            Result insertListingResult = insertLock(transactionId, "Listings");
            if (!insertListingResult.getStatus()) {
                rollback(transactionId);
                return null;
            }
            String listingRecordId = insertListingResult.getMessage();
            insert(transactionId, "Listings",  IId + "," + price, listingRecordId);
            commit(transactionId);
            return listingRecordId;
        }

        return null;
    }


    //    remoteCalls : 4
    public String addListingBamboo(String PId, String IId, double price) {

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

            if (!lock(transactionId, "Items", "IId", IId, READ_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            Map<String, String> item = read(transactionId, "Items", "IId", IId);
            if (item.isEmpty()) {
                log.info("Item does not exists!");
                commit(transactionId);
                return null;
            }
//             Check the owner
            if (Integer.parseInt( item.get("iowner")) != Integer.parseInt(PId)) {
                log.info("item has a different owner! {}<>{}", item.get("iowner"), PId);
                commit(transactionId);
                return null;
            }


            if (!lock(transactionId, "Players", "PId", PId, READ_TYPE).getStatus()) {
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

            Result insertListingResult = insertLock(transactionId, "Listings");
            if (!insertListingResult.getStatus()) {
                rollback(transactionId);
                return null;
            }
            String listingRecordId = insertListingResult.getMessage();
            insert(transactionId, "Listings",  IId + "," + price, listingRecordId);
//
            if (!waitForCommit(transactionId).getStatus()) {
                rollback(transactionId);
                return null;
            }

            commit(transactionId);
            return listingRecordId;
        }

        return null;
    }



    public String buyListingBamboo(String PId, String LId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();

            //            R from L where Lid
            if (!lock(transactionId, "Listings", "LId", LId, READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Players", "PId", PId, READ_TYPE).getStatus()) {
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
            if (Double.parseDouble(player.get("pcash")) < Double.parseDouble(listing.get("lprice"))) {
                log.info("player does not have enough cash");
                commit(transactionId);
                return null;
            }


//            Read from I where LIID
            if (!lock(transactionId, "Items", "IId", listing.get("liid"), READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Listings", "LId", LId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            delete(transactionId, "Listings", "LId", LId);


//            R from P where ppid
            String PPId = item.get("iowner");
            if (!lock(transactionId, "Players", "PId", PPId, READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Items", "IId", listing.get("liid"), WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            if (!write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId)) {
                rollback(transactionId);
                return null;
            }

            if (!retireLock(transactionId, "Items", "IId", listing.get("liid")).getStatus()) {
                rollback(transactionId);
                return null;
            }

//           W into P where Pid SET pCash = new Cash
            if (!lock(transactionId, "Players", "PId", PId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            String newCash = Double.toString(Double.parseDouble(player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            if (!write(transactionId, "Players", "PId", PId, "Pcash", newCash)) {
                rollback(transactionId);
                return null;
            }

//           W into P where Pid SET pCash = new Cash
            if (!lock(transactionId, "Players", "PId", PPId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
            if (!write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash)) {
                rollback(transactionId);
                return null;
            }


            if (!waitForCommit(transactionId).getStatus()) {
                rollback(transactionId);
                return null;
            }

//            Unlock
            commit(transactionId);
            return listing.get("liid");
        }
        return null;
    }

    private Result waitForCommit(String transactionId) {
        Result result = blockingStub.bambooWaitForCommit(TransactionId.newBuilder().setId(transactionId).build());
        log.info("{}, waitForCommit status: {} - message: {}",transactionId, result.getStatus(), result.getMessage());
        return result;
    }

    private Result retireLock(String transactionId, String tableName, String key, String value) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result retireResult = blockingStub.bambooRetireLock(lockData);
        log.info("{}, retire the lock {},{}:{} status: {} - message: {}",transactionId, tableName, key, value, retireResult.getStatus(), retireResult.getMessage());
        return retireResult;
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

            String listingRecordId = insertLock(transactionId, "Listings").getMessage();

            lock(transactionId, "Items", "IId", IId, READ_TYPE);
            Map<String, String> item = read(transactionId, "Items", "IId", IId);
//             Check the owner
            if (Integer.parseInt( item.get("iowner")) != Integer.parseInt(PId)) {
                log.error("item has a different owner! {}<>{}", item.get("iowner"), PId);
                commit(transactionId);
                return null;
            }

            lock(transactionId, "Players", "PId", PId, READ_TYPE);
            unlock(transactionId, "Items", "IId", IId);
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
    public String buyListing(String PId, String LId) {
        return switch (mode) {
            case "slw" -> buyListingSLW(PId, LId);
            case "bamboo" -> buyListingBamboo(PId, LId);
            case "ww" -> buyListingWW(PId, LId);
            default -> null;
        };
    }
    public String buyListingWW(String PId, String LId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();

        //            R from L where Lid
            if (!lock(transactionId, "Listings", "LId", LId, READ_TYPE).getStatus()) {
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
            if(!lock(transactionId, "Players", "PId", PId, READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Items", "IId", listing.get("liid"), READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Listings", "LId", LId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            delete(transactionId, "Listings", "LId", LId);


//            R from P where ppid
            String PPId =  item.get("iowner");
            if (!lock(transactionId, "Players", "PId", PPId, READ_TYPE).getStatus()) {
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
            if (!lock(transactionId, "Items", "IId", listing.get("liid"), WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            if (!write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId)) {
                rollback(transactionId);
                return null;
            }

//           W into P where Pid SET pCash = new Cash
            if (!lock(transactionId, "Players", "PId", PId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            String newCash = Double.toString(Double.parseDouble( player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            if(!write(transactionId, "Players", "PId", PId, "Pcash", newCash)) {
                rollback(transactionId);
                return null;
            }

//           W into P where Pid SET pCash = new Cash
            if (!lock(transactionId, "Players", "PId", PPId, WRITE_TYPE).getStatus()) {
                rollback(transactionId);
                return null;
            }
            String prevOwnerNewCash = Double.toString(Double.parseDouble( prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
            if(!write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash)) {
                rollback(transactionId);
                return null;
            }

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
//            unlock(transactionId, "Listings", "LId", LId);

//          W into I where IId = Liid SET Iowner = pid
            write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);
            unlock(transactionId, "Items", "IId", listing.get("liid"));

//            delay(3000);
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

    private Result unlock(String transactionId, String tableName, String key, String value) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result unlockResult = blockingStub.unlock(lockData);
        log.info("{}, unlock {},{}:{} status: {} - message: {}",transactionId, tableName, key, value, unlockResult.getStatus(), unlockResult.getMessage());
        if (locks.containsKey(transactionId)) {
            locks.get(transactionId).remove(lockData.getKey());
        }
        return unlockResult;
    }



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
    private Result commit(String transactionId) {
        Result commitResult = blockingStub.commitTransaction(TransactionId.newBuilder().setId(transactionId).build());
        log.info("commit {}: {}, {}", transactionId, commitResult.getStatus(), commitResult.getMessage());
        locks.remove(transactionId);
        return commitResult;
    }

    private boolean write(String transactionId, String tableName, String key, String value, String newKey, String newValue) {
        Data writeData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .setValue(newKey + "," + newValue)
                .build();
        Result writeResult = performRemoteOperation(writeData);
        log.info("Write to {} status: {}", tableName, writeResult.getStatus());
        return writeResult.getStatus();
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
        log.info("{}: read from {} status : {}",transactionId, tableName, readResult.getStatus());
        return readResult.getStatus() ? convertStringToMap(readResult.getMessage()) : new HashMap<>();
    }


    private boolean insert(String transactionId, String tableName, String newRecord, String recordId) {
        Data insertData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .setRecordId(recordId)
                .build();
        Result result = blockingStub.update(insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
        return result.getStatus();
    }

//    private boolean insert(String transactionId, String tableName, String newRecord) {
//        Data insertData = Data.newBuilder()
//                .setTransactionId(transactionId)
//                .setType(INSERT_TYPE)
//                .setKey(tableName)
//                .setValue(newRecord)
//                .build();
//        Result result = blockingStub.lockAndUpdate(insertData);
//        log.info("insert data with into {} status : {}", tableName, result.getStatus());
//        return result.getStatus();
//    }
    private Result lock(String transactionId, String tableName, String key, String value, String type) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(type)
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

    private Result lock(String transactionId, String tableName, String key, String value) {
        return lock(transactionId, tableName, key, value, WRITE_TYPE);
    }

    private Result insertLock(String transactionId, String tableName) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("lock on {} for insert status: {}", tableName, lockResult.getStatus());
        return lockResult;
    }
    private Result insertLock(String transactionId, String tableName, String recordId) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setRecordId(recordId)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("lock on {} for insert status: {}", tableName, lockResult.getStatus());
        return lockResult;
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


