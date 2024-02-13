package edgelab.retryFreeDB.slwGraph;

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxCompactTreeLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.layout.mxOrganicLayout;
import com.mxgraph.layout.mxParallelEdgeLayout;
import com.mxgraph.layout.mxStackLayout;
import com.mxgraph.util.mxCellRenderer;
import edgelab.retryFreeDB.slwGraph.nodes.LockNode;
import edgelab.retryFreeDB.slwGraph.nodes.TXNode;
import edgelab.retryFreeDB.slwGraph.nodes.UnlockNode;
import edgelab.retryFreeDB.slwGraph.nodes.UpdateNode;
import org.checkerframework.checker.units.qual.C;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.alg.cycle.HawickJamesSimpleCycles;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class SLWGraph {

    private final Graph<TXNode, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
    /*
    * Gets a transaction in forms of READ -> Write -> ...
    * */
    public void addNewTransaction(List<UpdateNode> transaction) {
        List<TXNode> transactionNodes = getTransactionNodes(transaction);

        for (TXNode txNode : transactionNodes)
            graph.addVertex(txNode);

        for (int i = 1; i < transactionNodes.size(); i++)
            graph.addEdge(transactionNodes.get(i-1), transactionNodes.get(i));

        addConflictEdges(transactionNodes);
    }

    private void addConflictEdges(List<TXNode> transactionNodes) {
        for(TXNode txNode: transactionNodes) {
            if (txNode instanceof LockNode) {
                for (TXNode toCheck: graph.vertexSet()) {
                    if (toCheck == txNode)
                        continue;

                    if (toCheck instanceof LockNode &&
                            ((LockNode) txNode).getTableName().equals(((LockNode) toCheck).getTableName())) {
                        graph.addEdge(toCheck, txNode);
                        graph.addEdge(txNode, toCheck);
                    }
                }

            }
        }
    }

    private List<TXNode> getTransactionNodes(List<UpdateNode> transaction) {
        Set<String> alreadyLocked = new HashSet<>();

        List<TXNode> transactionWithLocks = new ArrayList<>();
        for (UpdateNode node : transaction) {
            if (!alreadyLocked.contains(node.getTableName())) {
                alreadyLocked.add(node.getTableName());
                transactionWithLocks.add(LockNode.builder().tableName(node.getTableName()).build());
            }
            transactionWithLocks.add(node);
        }
        transactionWithLocks.add(UnlockNode.builder().toUnlockTables(alreadyLocked.stream().toList()).build());
        return transactionWithLocks;
    }


    public Boolean isThereADeadlock() {
        HawickJamesSimpleCycles <TXNode, DefaultEdge> cycleDetector = new HawickJamesSimpleCycles<>(graph);
        System.out.println(cycleDetector.findSimpleCycles());
        List<List<TXNode> > allCycles = cycleDetector.findSimpleCycles();
        for (List<TXNode> cycle :
                allCycles) {
            if (!areAllNodesSameLocks(cycle))
                return true;
        }
        return false;
    }

    private boolean areAllNodesSameLocks(List<TXNode> cycle) {
        for (int i = 1; i < cycle.size(); i++) {
            if (!(cycle.get(i-1) instanceof LockNode))
                return false;
            if (!(cycle.get(i) instanceof LockNode))
                return false;

            if (!((LockNode) cycle.get(i - 1)).getTableName().equals(((LockNode) cycle.get(i)).getTableName()))
                return false;
        }
        return true;
    }


    public void show() {
        File imgFile = new File("./graph.png");
        try {
            imgFile.createNewFile();
            JGraphXAdapter<TXNode, DefaultEdge> graphAdapter =
                    new JGraphXAdapter<TXNode, DefaultEdge>(graph);
            mxIGraphLayout layout = new mxCircleLayout(graphAdapter);
            layout.execute(graphAdapter.getDefaultParent());

            BufferedImage image =
                    mxCellRenderer.createBufferedImage(graphAdapter, null, 2, Color.WHITE, true, null);

            ImageIO.write(image, "PNG", imgFile);

        } catch (IOException e) {
            return;
        }
    }
}

