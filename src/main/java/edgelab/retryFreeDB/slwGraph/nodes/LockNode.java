package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
public class LockNode extends TXNode{
    @Setter
    @Getter
    protected String tableName;


    @Override
    public String toString() {
        return "Lock " + tableName;
    }
}
