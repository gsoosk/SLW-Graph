package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class UpdateNode extends TXNode{

    public UpdateNode(String tableName) {
        this.tableName = tableName;
    }

    @Setter
    protected String tableName;



}
