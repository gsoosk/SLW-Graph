package edgelab.retryFreeDB.repo.storage.DTO;

import edgelab.proto.Transaction;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

@Slf4j
public class DBTransaction {
    private static final String READ_TYPE = "read";
    private static final String WRITE_TYPE = "write";
    private static final String DELETE_TYPE = "delete";
    @Setter
    @Getter
    private List<DBData> dataList;

    public DBTransaction(List<DBData> dList) {
        dataList = dList;
    }

    public static DBTransaction deserialize(Transaction transaction) {
        List<DBData> dList = new ArrayList<>();

        for (edgelab.proto.Data data:
             transaction.getReadWriteList()) {
            DBData d = deserializeData(data);
            if (d != null) {
                dList.add(d);
            }

        }
        return new DBTransaction(dList);
    }

    public static DBData deserializeData(edgelab.proto.Data data) {
        String type = data.getType();
        String[] keys = data.getKey().split(",");
        if(keys.length != 3) {
            log.error("could not deserialize: not enough keys in Key string");
            return null;
        }
        DBData d;

        if (type.equals(WRITE_TYPE))
            d = new DBWriteData();
        else if(type.equals(DELETE_TYPE))
            d = new DBDeleteData();
        else
            d = new DBData();

        d.setTable(keys[0]);
        d.setId(keys[1]);
        d.setQuery(keys[2]);
        if (d instanceof DBWriteData) {
            String[] writeKeys = data.getValue().split(",");
            ((DBWriteData) d).setVariable(writeKeys[0]);
            ((DBWriteData) d).setValue(writeKeys[1]);
        }


        return d;
    }


}
