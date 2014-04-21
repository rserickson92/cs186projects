package simpledb;

import java.io.*;
import java.util.*;

/**
 * Locks manages all data regarding locks. Given a page ID, it can return all
 * locks on the page. Given a transaction ID, it can return all locks owned by
 * the transaction.
 */
class Locks {
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
           !(locks_by_tid.size() == 1 && locks_by_tid.get(tid) == LockType.S) &&
           locks_by_tid.get(tid) != LockType.X) {
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
                bt = waiting_transactions.getFirst();
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
    public  boolean locked(PageId pid, TransactionId tid) { return false; }
    public  boolean locked(TransactionId tid, PageId pid) {
        HashMap<PageId, LockType> locks_by_pid = transaction_locks.get(tid);
        return locks_by_pid != null && locks_by_pid.containsKey(pid);
    }
}

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */

public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
      other classes. BufferPool should use the numPages argument to the
      constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private HashMap<PageId, Page> buffer_pool;
    private Locks locks;
    private int max_pages;
    private ArrayList<PageId> lru_queue;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        max_pages = numPages;
        buffer_pool = new HashMap<PageId, Page>(max_pages);
        lru_queue = new ArrayList<PageId>(max_pages);
        locks = new Locks();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
            // some code goes here
        try {
            if(perm == Permissions.READ_ONLY) {
                while(!locks.addSharedLock(pid, tid)) {
                    Thread.sleep(100);
                }
            } else {
                while(!locks.addExclusiveLock(pid, tid)) {
                    Thread.sleep(100);
                }
            }
        } catch(InterruptedException e) {
            throw new DbException("interruption in getPage()");
        }
         
        if(buffer_pool.containsKey(pid)) {

            //move this (MRU) page to back of queue
            lru_queue.remove(lru_queue.indexOf(pid));
            lru_queue.add(pid);

            return buffer_pool.get(pid);
        }
        if(buffer_pool.size() >= max_pages) {
            evictPage();
        }
        Catalog gc = Database.getCatalog();
        int table_id = pid.getTableId();
        DbFile file = gc.getDbFile(table_id);
        buffer_pool.put(pid, file.readPage(pid));
        lru_queue.add(pid);
        return buffer_pool.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        locks.unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return locks.locked(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
            // some code goes here
            // not necessary for proj1
        }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
            // some code goes here
            DbFile table = Database.getCatalog().getDbFile(tableId);
            ArrayList<Page> dirty_pages = table.insertTuple(tid, t);
            for(Page dp : dirty_pages) {
                dp.markDirty(true, tid);
                buffer_pool.put(dp.getId(), dp);
            }
        }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
            // some code goes here
            RecordId rid = t.getRecordId();
            if(rid == null) {
                throw new DbException("Tuple has no recordId");
            }
            HeapPage p = (HeapPage) buffer_pool.get(rid.getPageId());
            if(p != null) { 
                p.deleteTuple(t); 
                p.markDirty(true, tid);
            } else {
                throw new DbException("Tuple is not in buffer pool");
            }
        }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for(PageId pid : buffer_pool.keySet()) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
      Needed by the recovery manager to ensure that the
      buffer pool doesn't keep a rolled back page in its
      cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Catalog gc = Database.getCatalog();
        int table_id = pid.getTableId();
        DbFile file = gc.getDbFile(table_id);
        Page p = buffer_pool.get(pid);
        file.writePage(p);
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        PageId pid = lru_queue.remove(0);
        Page p = buffer_pool.remove(pid);
        TransactionId dirty = p.isDirty();
        if(dirty != null) {
            try {
                flushPage(pid);
            } catch(IOException e) {
                throw new DbException("error flushing dirty page");
            }
        }
    }

}
