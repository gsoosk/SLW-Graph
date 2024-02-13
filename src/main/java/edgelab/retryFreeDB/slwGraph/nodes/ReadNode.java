package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;

public class ReadNode extends UpdateNode{
    public ReadNode(String tableName) {
        super(tableName);
    }

    @Override
    public String toString() {
        return "Read " + tableName;
    }
}
