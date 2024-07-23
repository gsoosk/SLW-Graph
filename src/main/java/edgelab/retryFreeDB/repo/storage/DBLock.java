package edgelab.retryFreeDB.repo.storage;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;

class DBLock {
    @Override
    public String toString() {
        return "Lock<" + resource + ", holding by: " + holdingTransactions + ", pending: " + pendingTransactions + ", next in line "+pendingTransactions.peek()+">";
    }

    private final String resource;
    private final Set<DBTransaction> holdingTransactions;
//    private final Map<String, LockType> transactionLockTypes;
    private LockType ownersLockType;
    private final Queue<DBTransaction> retiredTransactions;
    private final Map<DBTransaction, LockType> retiredLockTypes;
    private final Queue<DBTransaction> pendingTransactions;
    private final Map<DBTransaction, LockType> pendingLockTypes;


    private static final boolean BAMBOO_ENABLE = true;

    public DBLock(String resource) {
        this.resource = resource;
        this.holdingTransactions = new HashSet<>();
        if (BAMBOO_ENABLE) {
            this.pendingTransactions = new PriorityQueue<>(new Comparator<DBTransaction>() {
                @Override
                public int compare(DBTransaction s1, DBTransaction s2) {
                    return Long.compare(Long.parseLong(s1.getTimestamp()) ,Long.parseLong(s2.getTimestamp()));
                }
            });
            this.retiredTransactions =  new PriorityQueue<>(new Comparator<DBTransaction>() {
                @Override
                public int compare(DBTransaction s1, DBTransaction s2) {
                    return Long.compare(Long.parseLong(s1.getTimestamp()) ,Long.parseLong(s2.getTimestamp()));
                }
            });
            this.retiredLockTypes = new HashMap<>();
        }
        else
            this.pendingTransactions = new LinkedList<>();
        this.ownersLockType = null;
        this.pendingLockTypes = new HashMap<>();
    }

    public synchronized boolean canGrant(DBTransaction transaction, LockType lockType) {
        if (isHeldBefore(transaction, lockType))
            return true;


        if (lockType == LockType.WRITE) {
//            either no lock held or the same tx has a read lock
            if (holdingTransactions.isEmpty() || (holdingTransactions.size() == 1 && holdingTransactions.contains(transaction)))
                    if (pendingTransactions.contains(transaction) && pendingTransactions.peek().equals(transaction))
                        return true;
        }
        else {
            // Can grant read lock if no write lock is held
            if (ownersLockType == LockType.WRITE)
                return false;
            // Can grant read lock if no pending write lock is ahead
            for (DBTransaction value : pendingTransactions) {
                if (value.equals(transaction)) {
                    return true; // Reached the target value without finding any write values
                }
                if (pendingLockTypes.get(value).equals(LockType.WRITE)) {
                    return false;
                }
            }
            return true;
        }



        return false;
    }


    public synchronized void promoteWaiters() {
        while (!pendingTransactions.isEmpty()) {
            DBTransaction t = pendingTransactions.peek();

//            if (!isHeldBefore(t, pendingLockTypes.get(t))) // if pending is already taken
//                if (!holdingTransactions.isEmpty()) // no one holding
//                    if (!isUpgradeCase(t)) // same tx has the read lock
//                        if (conflict(pendingLockTypes.get(t), ownersLockType))
//                            break;
//
            if (canGrant(t, pendingLockTypes.get(t)))
                grant(t, pendingLockTypes.get(t));
            else
                break;


            if (BAMBOO_ENABLE) {
                for (DBTransaction tp : retiredTransactions) {
                    if (conflict(retiredLockTypes.get(tp), pendingLockTypes.get(t))) {
//                        t.commit_semaphore++;
                        break;
                    }
                }
            }
        }
    }

    public synchronized boolean conflict(DBTransaction holdingTransaction, DBTransaction transaction, LockType lockType) {
        if (holdingTransaction.equals(transaction))
            return false;
        return lockType == LockType.WRITE || ownersLockType == LockType.WRITE;
    }


    public synchronized boolean conflict(LockType lockType, LockType lockType2) {
        return lockType == LockType.WRITE || lockType2 == LockType.WRITE;
    }



    public synchronized void grant(DBTransaction transaction, LockType lockType) {
        pendingTransactions.remove(transaction);
        pendingLockTypes.remove(transaction);
        holdingTransactions.add(transaction);
        ownersLockType = lockType;
    }

    public synchronized void release(DBTransaction transaction) {
        holdingTransactions.remove(transaction);
        if (holdingTransactions.isEmpty())
            ownersLockType = null;
        pendingTransactions.remove(transaction);
        pendingLockTypes.remove(transaction);
    }


    public void retire(DBTransaction transaction) {
//         TODO
        holdingTransactions.remove(transaction);
        retiredTransactions.add(transaction);
        retiredLockTypes.put(transaction, ownersLockType);
        if (holdingTransactions.isEmpty())
            ownersLockType = null;


    }

    public String getResource() {
        return resource;
    }

    public synchronized Set<DBTransaction> getHoldingTransactions() {
        return new HashSet<>(holdingTransactions);
    }


    public synchronized void addPending(DBTransaction transaction, LockType lockType) {
        if (!pendingTransactions.contains(transaction)) {
            pendingTransactions.offer(transaction);
            pendingLockTypes.put(transaction, lockType);
        }
        else {
            if (pendingLockTypes.get(transaction).equals(LockType.READ) && lockType.equals(LockType.WRITE))
                pendingLockTypes.put(transaction, lockType); //Upgrade the pending lock
        }
    }


    public boolean isHeldBefore(DBTransaction transaction, LockType lockType) {
        return holdingTransactions.contains(transaction) && ownersLockType == lockType;
    }

    public String printPendingLocks() {
        StringBuilder s = new StringBuilder();
        s.append("[ ");
        for (DBTransaction p : pendingTransactions) {
            s.append(p).append(":").append(pendingLockTypes.get(p));
            s.append(", ");
        }
        s.append("]");
        return s.toString();
    }

}

enum LockType {
    READ,
    WRITE
}
