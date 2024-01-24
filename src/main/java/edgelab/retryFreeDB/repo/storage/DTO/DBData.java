package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;

@Data
public class DBData {
    private String table;
    private String id;
    private String query;

}

