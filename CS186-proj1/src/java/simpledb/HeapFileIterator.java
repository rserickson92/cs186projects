package simpledb;

public static class HeapFileIterator implements DbFileIterator {

    private Iterator<Tuple> i;
    private int page_no;
    private BufferPool buffer_pool;
    private HeapFile file;
    private TransactionId tid;

    public HeapFileIterator(HeapFile file, TransactionId tid) {
	i = null;
	this.file = file;
	this.tid = tid;
    }

    private void setPageIterator() {
	HeapPageId pid = new HeapPageId(file.getId(), page_no);
	HeapPage page = buffer_pool.getPage(tid, pid, Permissions.READ_ONLY);
	i = page.iterator();
    }
   
    public void open() 
    throws DbException, TransactionAbortedException {
	page_no = 0;
	buffer_pool = Database.getBufferPool();
	setPageIterator();
    }
    public boolean hasNext()
    throws DbException, TransactionAbortedException {
	return !i.hasNext() && page_no >= file.numPages();
    }
    public Tuple next()
    throws DbException, TransactionAbortedException, NoSuchElementException {
	if(i.hasNext()) {
	    return i.next();
	} else if(page_no < file.numPages()) { //get next page
	    page_no++;
	    setPageIterator();
	    return i.next();
	} else {
	    throw new NoSuchElementException("out of tuples in heap file");
	}
    }
    public void rewind()
    throws DbException, TransactionAbortedException {
	close();
	open();
    }
    public void close() {
	i = null;
	buffer_pool = null;
    }
}
