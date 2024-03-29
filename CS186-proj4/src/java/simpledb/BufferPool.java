package simpledb;

import java.io.*;
import java.util.*;

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
    private int max_pages;
    private ArrayList<PageId> lru_queue;
    private LockManager locks;
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
        locks = new LockManager();
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
            // some code goes here
            boolean blocked = true;
            //try {
                while(blocked) {
                    //Thread.sleep(100);
                    blocked = (perm == Permissions.READ_ONLY)
                            ? (!locks.addSharedLock(pid, tid))
                            : (!locks.addExclusiveLock(pid, tid));
                }
            //} catch(InterruptedException e) {
            //    throw new DbException("getPage interrupted");
            //}
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
        return locks.isLocked(p, tid);
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
