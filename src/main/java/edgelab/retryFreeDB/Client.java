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

            try {
                lock(tx5, "Players", "PId", "10", WRITE_TYPE);

                retireLock(tx5, "Players", "PId", "10");

                lock(tx6, "Players", "PId", "10", WRITE_TYPE);
                retireLock(tx6, "Players", "PId", "10");


                lock(tx4, "Players", "PId", "10", WRITE_TYPE);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        });
        delay();

        executor.submit(() -> {
            try {
                waitForCommit(tx5);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            commit(tx5);
        });

        executor.submit(() -> {
            try {
                waitForCommit(tx4);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            commit(tx4);
        });

        executor.submit(() -> {
            try {
                waitForCommit(tx3);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            commit(tx3);
        });

        executor.submit(() -> {
            try {
                waitForCommit(tx6);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
    public boolean TPCC_newOrder(String warehouseId, String districtId, String customerId, Integer orderLineCount, String allLocals, int[] itemIDs,  int[] supplierWarehouseIDs, int[] orderQuantities) {
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {

            String tx = initResult.getMessage();
            try {
//              Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                lock(tx, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(tx, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");


//              Get warehouse
                lock(tx, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(tx, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty())
                    throw new Exception("warehouse does not exists");



//              Get district
                lock(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");

//              Update district
                lock(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                String d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                write(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);

//              Insert OpenOrder
                String openOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(tx, "oorder", openOrderId);
                insert(tx, "oorder", customerId + "," +
                        "null" + "," +
                        orderLineCount + "," +
                        allLocals + "," +
                        "'" + new Timestamp(System.currentTimeMillis()) + "',"
                        ,openOrderId);

//              Insert NewOrder
                String newOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(tx, "new_order", newOrderId);
                insert(tx, "new_order", "", newOrderId);

                for (int ol_number = 0; ol_number < orderLineCount; ol_number++) {
                    int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                    int ol_i_id = itemIDs[ol_number - 1];
                    int ol_quantity = orderQuantities[ol_number - 1];

//                    Get Item Price
                    lock(tx, "item", "i_id", String.valueOf(ol_i_id), READ_TYPE);
                    Map<String, String> item = read(tx, "item", "i_id", String.valueOf(ol_i_id));

                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock
                    lock(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, READ_TYPE);
                    Map<String, String> stock = read(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (ol_supply_w_id != Integer.parseInt(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }


//                    Insert OrderLine
                    String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + ol_number;
                    insertLock(tx, "order_line", orderLineId);
                    insert(tx, "order_line", ol_i_id + "," + "null" + "," + ol_amount +
                            "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);

//                    Update Stock
                    lock(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE);

                    write(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, "s_quantity", newStockQuantity);
                    write(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, "s_ytd", String.valueOf(Integer.parseInt(stock.get("s_ytd")) + ol_quantity));
                    write(tx, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    write(tx,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, "s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));
                }

                commit(tx);
                return true;

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(tx);
                return false;
            }
        }

        return false;
    }
    private String getDistInfoKey(String districtId) {
        return switch (districtId) {
            case "1" -> "s_dist_01";
            case "2" -> "s_dist_02";
            case "3" -> "s_dist_03";
            case "4" -> "s_dist_04";
            case "5" -> "s_dist_05";
            case "6" -> "s_dist_06";
            case "7" -> "s_dist_07";
            case "8" -> "s_dist_08";
            case "9" -> "s_dist_09";
            case "10" -> "s_dist_10";
            default -> null;
        };
    }


    public boolean TPCC_payment(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId) {
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String tx = initResult.getMessage();
            try {


//            Get warehouse
                lock(tx, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(tx, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty()) {
                    log.info("warehouse does not exists");
                    commit(tx);
                    return false;
                }

//            update warehouse
                lock(tx, "warehouse", "w_id", warehouseId, WRITE_TYPE);
                float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                write(tx, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;



//          get district
                lock(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");
    //            Update District
                lock(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                write(tx, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;

    //            Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;
                lock(tx, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(tx, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");

//            Update customer
                lock(tx, "customer", customerIdKey, customerIdValue, WRITE_TYPE);
                float c_balance = Float.parseFloat(customer.get("c_balance")) - paymentAmount;
                float c_ytd_payment = Float.parseFloat(customer.get("c_ytd_payment")) + paymentAmount;
                int c_payment_cnt = Integer.parseInt(customer.get("c_payment_cnt")) + 1;
                write(tx, "customer", customerIdKey, customerIdValue, "c_balance", String.valueOf(c_balance));
                write(tx, "customer", customerIdKey, customerIdValue, "c_ytd_payment", String.valueOf(c_ytd_payment));
                write(tx, "customer", customerIdKey, customerIdValue, "c_payment_cnt", String.valueOf(c_payment_cnt));
                if (customer.get("c_credit").equals("BC")) {
                    String c_data = customerId + " " + customerDistrictId + " " + customerWarehouseId + " " + districtId + " " + warehouseId + " " + paymentAmount + " | " + customer.get("c_data");
                    if (c_data.length() > 500) {
                        c_data = c_data.substring(0, 500);
                    }

                    write(tx, "customer", customerIdKey, customerIdValue, "c_data", c_data);
                }

    //            insert history
                String historyId = customerId + "," + customerDistrictId + "," + customerWarehouseId + "," + districtId + "," + warehouseId;
//                No need for lock fo insert into history as it does not have a primary key
//                insertLock(tx, "history", historyId);
                String h_data = "'" + warehouseId + ":" + districtId + "'";
                insert(tx, "history", "'" + new Timestamp(System.currentTimeMillis()) + "'" + "," +
                        paymentAmount + "," +
                        h_data, historyId) ;


                commit(tx);
                return true;
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(tx);
                return false;
            }
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
        try {
            for (String IId :
                    items) {
                lock(tx, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(tx, "Items", "IId", IId);

            }
        } catch (Exception e) {
            rollback(tx);
            return true;
        }
        return false;
    }

    private boolean readItemBamboo(List<String> items, String tx) {
        if (readItems(items, tx)) return true;
        try {
           waitForCommit(tx);
        } catch (Exception e) {
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
        executor.submit(() -> {
            try {
                lock(tx1, "Players", "PId", "11");
            } catch (Exception e) {
                rollback(tx1);
            }
        });
        delay();
        executor.submit(() -> {

            try {
                lock(tx2, "Players", "PId", "10");
            } catch (Exception e) {
                rollback(tx2);
            }
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
            try {
//
//            Map<String, String> listing = read(transactionId, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(transactionId);
//                return;
//            }

                lock(transactionId, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(transactionId, "Items", "IId", IId);
                if (item.isEmpty()) {
                    log.info("Item does not exists!");
                    commit(transactionId);
                    return null;
                }
//             Check the owner
                if (Integer.parseInt(item.get("iowner")) != Integer.parseInt(PId)) {
                    log.info("item has a different owner! {}<>{}", item.get("iowner"), PId);
                    commit(transactionId);
                    return null;
                }

                lock(transactionId, "Players", "PId", PId, READ_TYPE);
                Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(transactionId);
                    return null;
                }

                Result insertListingResult = insertLock(transactionId, "Listings");
                String listingRecordId = insertListingResult.getMessage();
                insert(transactionId, "Listings", IId + "," + price, listingRecordId);
                commit(transactionId);
                return listingRecordId;
            } catch (Exception e) {
                rollback(transactionId);
                return null;
            }
        }

        return null;
    }


    //    remoteCalls : 4
    public String addListingBamboo(String PId, String IId, double price) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());
        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();
            try {
//
//            Map<String, String> listing = read(transactionId, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(transactionId);
//                return;
//            }

                lock(transactionId, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(transactionId, "Items", "IId", IId);
                if (item.isEmpty()) {
                    log.info("Item does not exists!");
                    commit(transactionId);
                    return null;
                }
//             Check the owner
                if (Integer.parseInt(item.get("iowner")) != Integer.parseInt(PId)) {
                    log.info("item has a different owner! {}<>{}", item.get("iowner"), PId);
                    commit(transactionId);
                    return null;
                }


                lock(transactionId, "Players", "PId", PId, READ_TYPE);
                Map<String, String> player = read(transactionId, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(transactionId);
                    return null;
                }

                Result insertListingResult = insertLock(transactionId, "Listings");
                String listingRecordId = insertListingResult.getMessage();
                insert(transactionId, "Listings", IId + "," + price, listingRecordId);
//
                waitForCommit(transactionId);

                commit(transactionId);
                return listingRecordId;
            } catch (Exception e) {
                rollback(transactionId);
                return null;
            }
        }

        return null;
    }



    public String buyListingBamboo(String PId, String LId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();
            try {

                //            R from L where Lid
                lock(transactionId, "Listings", "LId", LId, READ_TYPE);
                Map<String, String> listing = read(transactionId, "Listings", "LId", LId);
//            Check player exists
                if (listing.isEmpty()) {
                    log.info("listing does not exists!");
                    commit(transactionId);
                    return null;
                }

                //            Read from P where pid
                lock(transactionId, "Players", "PId", PId, READ_TYPE);
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
                lock(transactionId, "Items", "IId", listing.get("liid"), READ_TYPE);
                Map<String, String> item = read(transactionId, "Items", "IId", listing.get("liid"));
                //            Check item exists
                if (item.isEmpty()) {
                    log.info("item does not exists!");
                    commit(transactionId);
                    return null;
                }


//            Delete from L where LID
                lock(transactionId, "Listings", "LId", LId, WRITE_TYPE);
                delete(transactionId, "Listings", "LId", LId);


//            R from P where ppid
                String PPId = item.get("iowner");
                lock(transactionId, "Players", "PId", PPId, READ_TYPE);
                Map<String, String> prevOwner = read(transactionId, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.info("previous owner does not exists!");
                    commit(transactionId);
                    return null;
                }

//          W into I where IId = Liid SET Iowner = pid
                lock(transactionId, "Items", "IId", listing.get("liid"), WRITE_TYPE);
                write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);


                retireLock(transactionId, "Items", "IId", listing.get("liid"));

//           W into P where Pid SET pCash = new Cash
                lock(transactionId, "Players", "PId", PId, WRITE_TYPE);
                String newCash = Double.toString(Double.parseDouble(player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//           W into P where Pid SET pCash = new Cash
                lock(transactionId, "Players", "PId", PPId, WRITE_TYPE);
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);


                waitForCommit(transactionId);

//            Unlock
                commit(transactionId);
                return listing.get("liid");
            } catch (Exception e) {
                rollback(transactionId);
                return null;
            }
        }
        return null;
    }

    private Result waitForCommit(String transactionId) throws Exception {
        Result result = blockingStub.bambooWaitForCommit(TransactionId.newBuilder().setId(transactionId).build());
        log.info("{}, waitForCommit status: {} - message: {}",transactionId, result.getStatus(), result.getMessage());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
        return result;
    }

    private Result retireLock(String transactionId, String tableName, String key, String value) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result retireResult = blockingStub.bambooRetireLock(lockData);
        log.info("{}, retire the lock {},{}:{} status: {} - message: {}",transactionId, tableName, key, value, retireResult.getStatus(), retireResult.getMessage());
        if (!retireResult.getStatus())
            throw new Exception(retireResult.getMessage());
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
            try {
                String listingRecordId = insertLock(transactionId, "Listings").getMessage();

                lock(transactionId, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(transactionId, "Items", "IId", IId);
//             Check the owner
                if (Integer.parseInt(item.get("iowner")) != Integer.parseInt(PId)) {
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

                insert(transactionId, "Listings", IId + "," + price, listingRecordId);
                commit(transactionId);
                return listingRecordId;
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(transactionId);
                return null;
            }
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
            try {

                //            R from L where Lid
                lock(transactionId, "Listings", "LId", LId, READ_TYPE);
                Map<String, String> listing = read(transactionId, "Listings", "LId", LId);
//            Check player exists
                if (listing.isEmpty()) {
                    log.info("listing does not exists!");
                    commit(transactionId);
                    return null;
                }

                //            Read from P where pid
                lock(transactionId, "Players", "PId", PId, READ_TYPE);
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
                lock(transactionId, "Items", "IId", listing.get("liid"), READ_TYPE);
                Map<String, String> item = read(transactionId, "Items", "IId", listing.get("liid"));
                //            Check item exists
                if (item.isEmpty()) {
                    log.info("item does not exists!");
                    commit(transactionId);
                    return null;
                }


//            Delete from L where LID
                lock(transactionId, "Listings", "LId", LId, WRITE_TYPE);
                delete(transactionId, "Listings", "LId", LId);


//            R from P where ppid
                String PPId = item.get("iowner");
                lock(transactionId, "Players", "PId", PPId, READ_TYPE);
                Map<String, String> prevOwner = read(transactionId, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.info("previous owner does not exists!");
                    commit(transactionId);
                    return null;
                }

//          W into I where IId = Liid SET Iowner = pid
                lock(transactionId, "Items", "IId", listing.get("liid"), WRITE_TYPE);
                write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);

//           W into P where Pid SET pCash = new Cash
                lock(transactionId, "Players", "PId", PId, WRITE_TYPE);
                String newCash = Double.toString(Double.parseDouble(player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//           W into P where Pid SET pCash = new Cash
                lock(transactionId, "Players", "PId", PPId, WRITE_TYPE).getStatus();
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);

//            Unlock
                commit(transactionId);
                return listing.get("liid");
            } catch (Exception e) {
                rollback(transactionId);
                return null;
            }
        }
        return null;
    }

    public String buyListingSLW(String PId, String LId) {
        log.info("buy listing <PID:{}, LID:{}>", PId, LId);
        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();
            try {
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
                lock(transactionId, "Items", "IId", listing.get("liid"));
                Map<String, String> item = read(transactionId, "Items", "IId", listing.get("liid"));
                //            Check item exists
                if (item.isEmpty()) {
                    log.error("item does not exists!");
                    commit(transactionId);
                    return null;
                }

//            Lock Players where pid, ppid
                String PPId = item.get("iowner");
                if (Integer.parseInt(PPId) > Integer.parseInt(PId)) {
                    lock(transactionId, "Players", "PId", PId);
                    lock(transactionId, "Players", "PId", PPId);
                } else {
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
                if (Double.parseDouble(player.get("pcash")) < Double.parseDouble(listing.get("lprice"))) {
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
                String newCash = Double.toString(Double.parseDouble(player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//           W into P where Pid SET pCash = new Cash
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
                write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);

//            Unlock
                commit(transactionId);
                return listing.get("liid");
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(transactionId);
                return null;
            }
        }
        return null;
    }

    private Result unlock(String transactionId, String tableName, String key, String value) throws Exception {
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
        if (!unlockResult.getStatus())
            throw new Exception(unlockResult.getMessage());
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

    private void write(String transactionId, String tableName, String key, String value, String newKey, String newValue) throws Exception {
        Data writeData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .setValue(newKey + "," + newValue)
                .build();
        Result writeResult = performRemoteOperation(writeData);
        log.info("Write to {} status: {}", tableName, writeResult.getStatus());
        if (!writeResult.getStatus())
            throw new Exception(writeResult.getMessage());
    }

    private void delete(String transactionId, String tableName, String key, String value) throws Exception {
        Data deleteData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(DELETE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result deleteResult = performRemoteOperation(deleteData);
        if (!deleteResult.getStatus())
            throw new Exception(deleteResult.getMessage());
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


    private void insert(String transactionId, String tableName, String newRecord, String recordId) throws Exception {
        Data insertData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .setRecordId(recordId)
                .build();
        Result result = blockingStub.update(insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
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
    private Result lock(String transactionId, String tableName, String key, String value, String type) throws Exception {
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
        if (!lockResult.getStatus())
            throw new Exception(lockResult.getMessage());
        return lockResult;
    }

    private Result lock(String transactionId, String tableName, String key, String value) throws Exception {
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
    private Result insertLock(String transactionId, String tableName, String recordId) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setRecordId(recordId)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("lock on {} for insert status: {}", tableName, lockResult.getStatus());
        if (!lockResult.getStatus())
            throw new Exception(lockResult.getMessage());
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


