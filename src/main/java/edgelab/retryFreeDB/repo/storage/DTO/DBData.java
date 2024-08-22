package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;

import java.util.List;

@Data
public class DBData {
    private String table;
    private List<String> ids;
    private List<Integer> queries;
}
