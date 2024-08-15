package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Setter;

import java.util.List;
@Builder
public class UnlockNode extends TXNode {
    @Setter
    protected List<String> toUnlockTables;



    @Override
    public String toString() {
        return "Unlock " + toUnlockTables.toString() ;
    }
}
