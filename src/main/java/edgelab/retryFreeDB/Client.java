package edgelab.retryFreeDB;

import edgelab.proto.RetryFreeDBServerGrpc;
import io.grpc.Channel;
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
          client.buyListing("2", "1", "1");
    }

    private void buyListing(String PId, String LId, String PPId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();

//            Lock P where PPid
            lock(transactionId,"Players", "PId", PPId);

//            Read from P where pid
            Map<String, String> player = read(transactionId, "Players", "PId", PId);

//            R from L where Lid
            Map<String, String> listing = read(transactionId, "Listings", "LId", LId);

//            Check players cash for listing
            if (Double.parseDouble( player.get("pcash")) < Double.parseDouble(listing.get("lprice"))){
                log.info("player does not have enough cash");
                Result commitResult = blockingStub.commitTransaction(TransactionId.newBuilder().setId(transactionId).build());
            }

//          Delete From L where Lid
            delete(transactionId, "Listings", "LId", LId);

//          W into I where IId = Liid SET Iowner = pid
            write(transactionId, "Items", "IId", listing.get("liid"), "IOwner", PId);

//           W into P where Pid SET pCash = new Cash
            String newCash = Double.toString(Double.parseDouble( player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PId, "Pcash", newCash);

//            R from P where ppid
            Map<String, String> prevOwner = read(transactionId, "Players", "PId", PPId);

//           W into P where Pid SET pCash = new Cash
            String prevOwnerNewCash = Double.toString(Double.parseDouble( prevOwner.get("pcash")) + Double.parseDouble(listing.get("lprice")));
            write(transactionId, "Players", "PId", PPId, "Pcash", prevOwnerNewCash);

//            Unlock
            commit(transactionId);
        }


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

    private void lock(String transactionId, String tableName, String key, String value) {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result lockResult = blockingStub.lock(lockData);
        log.info("lock on {},{} status: {}", tableName, key, lockResult.getMessage());
        if (locks.containsKey(transactionId)) {
            locks.get(transactionId).add(lockData.getKey());
        }
        else {
            locks.put(transactionId, new HashSet<>());
            locks.get(transactionId).add(lockData.getKey());
        }
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


