package edgelab.retryFreeDB.repo.storage;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

class DBLock {
    @Override
    public String toString() {
        return "Lock<" + resource + ", holding by: " + holdingTransactions + ", pending: " + pendingTransactions + ", next in line "+pendingTransactions.peek()+">";
    }

    private final String resource;
    private final Set<String> holdingTransactions;
    private final Queue<String> pendingTransactions;
    private final Map<String, LockType> transactionLockTypes;

    private static final boolean BAMBOO_ENABLE = true;

    public DBLock(String resource) {
        this.resource = resource;
        this.holdingTransactions = new HashSet<>();
        if (BAMBOO_ENABLE) {
            this.pendingTransactions = new PriorityQueue<>(new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    return Long.compare(Long.parseLong(s1) ,Long.parseLong(s2));
                }
            });
        }
        else
            this.pendingTransactions = new LinkedList<>();
        this.transactionLockTypes = new HashMap<>();
    }

    public synchronized boolean canGrant(String transaction, LockType lockType) {
        if (holdingTransactions.contains(transaction))
            return true;
        if (holdingTransactions.isEmpty()) {
            if (pendingTransactions.isEmpty())
                return true;
            else if (pendingTransactions.peek().equals(transaction))
                return true;
        }
//        if (lockType == LockType.READ && holdingTransactions.size() == 1 &&
//                transactionLockTypes.get(holdingTransactions.iterator().next()) == LockType.READ) {
//            return true;
//        }
        return false;
    }

    public synchronized void grant(String transaction, LockType lockType) {
        if (!pendingTransactions.isEmpty() && pendingTransactions.peek().equals(transaction))
                pendingTransactions.poll();
        holdingTransactions.add(transaction);
        transactionLockTypes.put(transaction, lockType);
    }

    public synchronized void release(String transaction) {
        holdingTransactions.remove(transaction);
        transactionLockTypes.remove(transaction);
        pendingTransactions.remove(transaction);
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


    public boolean aborted(String tx) {
        return !holdingTransactions.contains(tx) && !pendingTransactions.contains(tx);
    }
}

enum LockType {
    READ,
    WRITE
}
