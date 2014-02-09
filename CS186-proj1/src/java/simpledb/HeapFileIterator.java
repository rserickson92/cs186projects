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

    private void setPageIterator() {
        if(i_pos != null) {
            i = i_pos;
            return;
        }
        PageId pid = new HeapPageId(file.getId(), page_no);
        HeapPage page = null;
        try {
            page = (HeapPage) buffer_pool.getPage(tid, pid, Permissions.READ_ONLY);
        } catch(Exception e) {
            e.printStackTrace();
        }
        i = page.iterator();
    }

    public void open() 
        throws DbException, TransactionAbortedException {
            setPageIterator();
        }
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
            return i != null && 
                   (i.hasNext() || page_no < file.numPages()-1);
        }
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
            if(i == null) {
                throw new NoSuchElementException("iterator is not open");
            } else if(i.hasNext()) {
                return i.next();
            } else if(page_no < file.numPages()-1) { //get next page
                page_no++;
                setPageIterator();
                return i.next();
            } else {
                throw new NoSuchElementException("no more tuples in file");
            }
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
