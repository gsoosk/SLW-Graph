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
import java.util.Map;

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
          client.buyListing("1", "1");
    }

    private void buyListing(String PId, String LId) {

        Result initResult = blockingStub.beginTransaction(Empty.newBuilder().build());

        if (initResult.getStatus()) {
            String transactionId = initResult.getMessage();


//            Lock Items just for

//            R from P where pid
            Data readFromP = Data.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(READ_TYPE)
                    .setKey("Players,PId,"+PId)
                    .build();
            Result readFromPResult = blockingStub.update(readFromP);
            log.info(readFromPResult.getMessage());

//            R from L where Lid
            Data readFromL = Data.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(READ_TYPE)
                    .setKey("Listings,LId,"+LId)
                    .build();
            Result readFromLResult = blockingStub.update(readFromL);
            log.info(readFromLResult.getMessage());

//            Check players cash for listing
            Map<String, String> player = convertStringToMap(readFromPResult.getMessage());
            Map<String, String> listing = convertStringToMap(readFromLResult.getMessage());
            if (Double.parseDouble( player.get("pcash")) < Double.parseDouble(listing.get("lprice"))){
                log.info("player does not have enough cash");
                Result commitResult = blockingStub.commitTransaction(TransactionId.newBuilder().setId(transactionId).build());
            }

//          Delete From L where Lid
            Data deleteFromL = Data.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(DELETE_TYPE)
                    .setKey("Listings,LId,1")
                    .build();
            Result deleteFromLResult = blockingStub.update(deleteFromL);
            log.info("delete from l status: {}", deleteFromLResult.getStatus());


//          W into I where IId = Lid SET Iowner = pid
            Data writeToI = Data.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(WRITE_TYPE)
                    .setKey("Items,IId," + LId)
                    .setValue("IOwner," + PId)
                    .build();
            Result writeToIResult = blockingStub.update(writeToI);
            log.info("Write to I status: {}", writeToIResult.getStatus());


//           W into P where Pid SET pCash = new Cash
            String newCash = Double.toString(Double.parseDouble( player.get("pcash")) - Double.parseDouble(listing.get("lprice")));
            Data writeToP = Data.newBuilder()
                    .setTransactionId(transactionId)
                    .setType(WRITE_TYPE)
                    .setKey("Players,PId," + PId)
                    .setValue("Pcash," + newCash)
                    .build();
            Result writeToPResult = blockingStub.update(writeToP);
            log.info("Write to P status: {}", writeToPResult.getStatus());
//            Unlock
            Result commitResult = blockingStub.commitTransaction(TransactionId.newBuilder().setId(transactionId).build());
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


