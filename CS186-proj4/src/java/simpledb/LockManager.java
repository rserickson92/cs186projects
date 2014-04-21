package simpledb;

import java.util.*;

/**
 * LockManager manages all data regarding locks. Given a page ID, it can return all
 * locks on the page. Given a transaction ID, it can return all locks owned by
 * the transaction.
 */
public class LockManager {
    class BlockedTransaction {
        public TransactionId tid;
        public LockType type;
        public BlockedTransaction(TransactionId tid, LockType type) {
            this.tid = tid;
            this.type = type;
        }
        public boolean equals(Object o) {
            if(!(o instanceof BlockedTransaction)) { return false; }
            BlockedTransaction bt = (BlockedTransaction) o;
            return tid.equals(bt.tid) && type == bt.type;
        }
    }
    private enum LockType { S, X }
    private HashMap<TransactionId, HashMap<PageId, LockType>> transaction_locks = 
        new HashMap<TransactionId, HashMap<PageId, LockType>>();
    private HashMap<PageId, HashMap<TransactionId, LockType>> page_locks =
        new HashMap<PageId, HashMap<TransactionId, LockType>>();
    private HashMap<PageId, LinkedList<BlockedTransaction>> blocked_transactions = 
        new HashMap<PageId, LinkedList<BlockedTransaction>>();

    private void enqueueTransaction(TransactionId tid, PageId pid, LockType type) {
        BlockedTransaction bt = new BlockedTransaction(tid, type);
        if(!blocked_transactions.containsKey(pid)) { 
            blocked_transactions.put(pid, new LinkedList<BlockedTransaction>());
        }
        blocked_transactions.get(pid).add(bt);
    }
    private void addLock(PageId pid, TransactionId tid, LockType type) {
        HashMap<TransactionId, LockType> locks_by_tid = page_locks.get(pid);
        if(locks_by_tid != null) {
            locks_by_tid.put(tid, type);
        } else {
            locks_by_tid = new HashMap<TransactionId, LockType>();
            locks_by_tid.put(tid, type);
            page_locks.put(pid, locks_by_tid);
        }

        HashMap<PageId, LockType> locks_by_pid = transaction_locks.get(tid);
        if(locks_by_pid != null) {
            locks_by_pid.put(pid, type);
        } else {
            locks_by_pid = new HashMap<PageId, LockType>();
            locks_by_pid.put(pid, type);
            transaction_locks.put(tid, locks_by_pid);
        }
    }
    /**
     * Attempts to acquire a shared lock on a page.
     *
     * @param pid the page ID the lock will be acquired on
     * @param tid the transaction ID attempting to acquire the lock
     * @return true if the attempt was successful, false if not
     */
    public boolean addSharedLock(PageId pid, TransactionId tid) {
        HashMap<TransactionId, LockType> locks_by_tid = page_locks.get(pid);
        if(locks_by_tid != null && locks_by_tid.containsValue(LockType.X)) { 
            enqueueTransaction(tid, pid, LockType.S);
            return false; 
        }
        addLock(pid, tid, LockType.S);
        return true;
    }
    /**
     * Attempts to acquire an exclusive lock on a page. If the transaction has
     * a shared lock and it is the only transaction with a lock, the lock will
     * be upgraded.
     *
     * @param pid the page ID the lock will be acquired on
     * @param tid the transaction ID attempting to acquire the lock
     * @return true if the attempt was successful, false if not
     */
    public boolean addExclusiveLock(PageId pid, TransactionId tid) {
        HashMap<TransactionId, LockType> locks_by_tid = page_locks.get(pid);

        //if there are locks on this page AND it's not the case that this
        //transaction holds the only lock and it's shared AND this transaction
        //doesn't already have an exclusive lock on the page
        if(locks_by_tid != null && !locks_by_tid.isEmpty() &&
           !(locks_by_tid.size() == 1 && locks_by_tid.containsKey(tid))) {
            enqueueTransaction(tid, pid, LockType.X);
            return false;
        }
        addLock(pid, tid, LockType.X);
        return true;
    }
    public void unlock(PageId pid, TransactionId tid) { 
        HashMap<TransactionId, LockType> locks_by_tid = page_locks.get(pid);
        HashMap<PageId, LockType> locks_by_pid = transaction_locks.get(tid);
        BlockedTransaction bt = null;
        if(locks_by_tid != null && locks_by_pid != null) {
            locks_by_tid.remove(tid);
            locks_by_pid.remove(pid);
            LinkedList<BlockedTransaction> waiting_transactions =
                blocked_transactions.remove(pid);
            while(waiting_transactions != null && waiting_transactions.size() > 0) {
                bt = waiting_transactions.remove();
                attemptToAddLock(pid, bt.tid, bt.type);
            }
        }

    }
    private void attemptToAddLock(PageId pid, TransactionId tid, LockType type) {
        if(type == LockType.S) {
            addSharedLock(pid, tid);
        } else {
            addExclusiveLock(pid, tid);
        }
    }
    public  boolean locked(TransactionId tid, PageId pid) {
        HashMap<PageId, LockType> locks_by_pid = transaction_locks.get(tid);
        return locks_by_pid != null && locks_by_pid.containsKey(pid);
    }
}
