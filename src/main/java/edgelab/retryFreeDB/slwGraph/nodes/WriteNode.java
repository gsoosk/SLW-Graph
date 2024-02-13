package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;

public class WriteNode extends UpdateNode{
    public WriteNode(String tableName) {
        super(tableName);
    }


    @Override
    public String toString() {
        return "Write " + tableName;
    }
}
