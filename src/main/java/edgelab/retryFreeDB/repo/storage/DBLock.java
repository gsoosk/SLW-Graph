package edgelab.retryFreeDB.repo.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

class DBLock {
    private final String resource;
    private final Set<String> holdingTransactions;
    private final LinkedList<String> pendingTransactions;
    private final Map<String, LockType> transactionLockTypes;

    public DBLock(String resource) {
        this.resource = resource;
        this.holdingTransactions = new HashSet<>();
        this.pendingTransactions = new LinkedList<>();
        this.transactionLockTypes = new HashMap<>();
    }

    public synchronized boolean canGrant(String transaction, LockType lockType) {
        if (holdingTransactions.contains(transaction))
            return true;
        if (holdingTransactions.isEmpty()) {
            if (pendingTransactions.isEmpty())
                return true;
            else if (pendingTransactions.getFirst().equals(transaction))
                return true;
        }
//        if (lockType == LockType.READ && holdingTransactions.size() == 1 &&
//                transactionLockTypes.get(holdingTransactions.iterator().next()) == LockType.READ) {
//            return true;
//        }
        return false;
    }

    public synchronized void grant(String transaction, LockType lockType) {
        if (!pendingTransactions.isEmpty() && pendingTransactions.getFirst().equals(transaction))
                pendingTransactions.poll();
        holdingTransactions.add(transaction);
        transactionLockTypes.put(transaction, lockType);
    }

    public synchronized void release(String transaction) {
        holdingTransactions.remove(transaction);
        transactionLockTypes.remove(transaction);
    }

    public String getResource() {
        return resource;
    }

    public synchronized Set<String> getHoldingTransactions() {
        return new HashSet<>(holdingTransactions);
    }


    public synchronized void addPending(String transaction) {
        if (!pendingTransactions.contains(transaction))
            pendingTransactions.offer(transaction);
    }


}

enum LockType {
    READ,
    WRITE
}
