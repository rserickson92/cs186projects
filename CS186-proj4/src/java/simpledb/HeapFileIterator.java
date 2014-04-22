package simpledb;

import java.util.*;
import java.io.*;

public class HeapFileIterator implements DbFileIterator {

    private Iterator<Tuple> i;
    private Iterator<Tuple> i_pos; //for reopening after close
    private int page_no;
    private BufferPool buffer_pool;
    private HeapFile file;
    private TransactionId tid;

    public HeapFileIterator(HeapFile file, TransactionId tid) {
        i = null;
        i_pos = null;
        this.file = file;
        this.tid = tid;
        buffer_pool = Database.getBufferPool();
        page_no = 0;
    }

    private void setPageIterator() throws DbException {
        if(i_pos != null) {
            i = i_pos;
            return;
        }
        //System.err.println(page_no);
        PageId pid = new HeapPageId(file.getId(), page_no);
        HeapPage page = null;
        try {
            page = (HeapPage) buffer_pool.getPage(tid, pid, Permissions.READ_ONLY);
        } catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            throw new DbException("error accessing page in heap file iterator");
        }
        i = page.iterator();
    }

    public void open() 
        throws DbException, TransactionAbortedException {
            setPageIterator();
        }
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
            if(i == null) { return false; }
            if(i.hasNext()) { return true; }
            PageId pid = null;
            HeapPage page = null;

            //scan subsequent pages for a next tuple
            for(int j = page_no+1; j < file.numPages(); j++) {
                pid = new HeapPageId(file.getId(), j);
                page = (HeapPage) buffer_pool.getPage(tid, pid, Permissions.READ_ONLY);
                if(page.getNumEmptySlots() != page.numSlots) {
                    return true;
                } else {
                    buffer_pool.releasePage(tid, pid);
                }
            }
            return false;
        }
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
            if(i == null) {
                throw new NoSuchElementException("iterator is not open");
            } else if(i.hasNext()) {
                return i.next();
            } 

            //keep going if entire page is blank
            while(page_no < file.numPages()-1) {
                page_no++;
                setPageIterator();
                if(i.hasNext()) { return i.next(); }
            }
            if(i.hasNext()) { return i.next(); }
            throw new NoSuchElementException("no more tuples in file");
        }
    public void rewind()
        throws DbException, TransactionAbortedException {
            close();
            i_pos = null;
            page_no = 0;
            open();
        }
    public void close() {
        i_pos = i;
        i = null;
    }
}
