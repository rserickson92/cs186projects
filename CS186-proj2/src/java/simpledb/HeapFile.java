package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return getFile().getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        RandomAccessFile raf = null;
        Page returnme = null;
        try { 
            raf = new RandomAccessFile(getFile(), "r");
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }

        byte[] data = new byte[BufferPool.PAGE_SIZE];
        long offset = (long) BufferPool.PAGE_SIZE * pid.pageNumber();
        try {
            raf.seek(offset);
            for(int i = 0; i < data.length; i++) {
                data[i] = raf.readByte();
            }
            returnme = new HeapPage((HeapPageId) pid, data);
        } catch(EOFException eofe) {
            eofe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } 
        return returnme;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        long offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
        RandomAccessFile raf = null;
        byte[] data = page.getPageData();
        try {
            raf = new RandomAccessFile(getFile(), "rw");
            raf.seek(offset);
            for(int i = 0; i < data.length; i++) {
                raf.writeByte(i);
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
            // some code goes here
            BufferPool bp = Database.getBufferPool();
            int id = getId(), i;
            ArrayList<Page> retlist = new ArrayList<Page>();
            PageId pid = null;
            HeapPage p = null;
            for(i = 0; i < numPages(); i++) {
                pid = new HeapPageId(id, i);
                p = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
                if(p.getNumEmptySlots() > 0) {
                    p.insertTuple(t);
                    retlist.add(p);
                    return retlist;
                }
            }

            //create new page and add tuple to it
            pid = new HeapPageId(id, i);
            RandomAccessFile raf = null;
            long offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            try {
                raf = new RandomAccessFile(getFile(), "rw");
                raf.setLength(raf.length() + BufferPool.PAGE_SIZE);
                raf.seek(offset);
                for(int j = 0; j < data.length; j++) {
                    data[j] = raf.readByte();
                }
            } catch(FileNotFoundException e) {
                e.printStackTrace();
            }
            p = new HeapPage((HeapPageId) pid, data);
            p.insertTuple(t);
            retlist.add(p);
            return retlist;
        }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
           TransactionAbortedException {
               // some code goes here
        BufferPool bp = Database.getBufferPool();
        RecordId rid = t.getRecordId();
        if(rid == null) {
            throw new DbException("Tuple is not a member of this file");
        }
        HeapPage p = (HeapPage) bp.getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        return p;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

