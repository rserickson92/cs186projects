package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private int tableid;
    private String alias;
    private DbFileIterator i;
    private DbFileIterator i_pos;
    private TupleDesc td;
    private TransactionId tid;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tid = tid;
        alias = tableAlias;
        i = null;
        i_pos = null;
        createAliasedTd();
    }

    private void createAliasedTd() {
        Catalog gc = Database.getCatalog();
        TupleDesc old_td = gc.getTupleDesc(tableid);
        String[] newFieldAr = new String[old_td.numFields()];
        Type[] typeAr = new Type[old_td.numFields()];
        String field = null;
        for(int i = 0; i < newFieldAr.length; i++) {
            field = old_td.getFieldName(i);
            if(alias == null) {
                alias = "null";
            } else if(field == null) {
                field = "null";
            }
            newFieldAr[i] = alias + "." + field;
            typeAr[i] = old_td.getFieldType(i);
        }
        td = new TupleDesc(typeAr, newFieldAr);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        alias = tableAlias;
        createAliasedTd();
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if(i_pos != null) {
            i = i_pos;
        } else {
            Catalog gc = Database.getCatalog();
            HeapFile file = (HeapFile) gc.getDbFile(tableid);
            i = file.iterator(tid);
        }
        i.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return i != null && i.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(i == null) {
            throw new NoSuchElementException("iterator is closed");
        }
        if(!i.hasNext()) {
            throw new NoSuchElementException("end of seq scan");
        }
        return i.next();
    }

    public void close() {
        // some code goes here
        i.close();
        i_pos = i;
        i = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        i.rewind();
        i_pos = null;
    }
}
