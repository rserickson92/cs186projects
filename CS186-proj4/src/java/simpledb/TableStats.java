package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private HashSet<PageId> pid_set = new HashSet<PageId>();
    private int page_io_cost;
    private Object[] histograms;
    private int[] mins;
    private int[] maxes;
    private int tuple_count = 0;
    private String[] str_fields;
    private TupleDesc td;

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        page_io_cost = ioCostPerPage;
        DbFile table = Database.getCatalog().getDbFile(tableid);
        DbFileIterator i = table.iterator(new TransactionId());
        td = table.getTupleDesc();
        int td_len = td.numFields();
        mins = new int[td_len];
        maxes = new int[td_len];
        str_fields = new String[td_len];
        for(int k = 0; k < td_len; k++) {
            mins[k] = Integer.MAX_VALUE;
            maxes[k] = Integer.MIN_VALUE;
        }
        histograms = new Object[td_len];

        //placeholder variables
        Tuple t = null;
        PageId pid = null;
        Field f = null;
        Type type = null;

        //compute min/max values, set pageID for scan cost estimation, count tuples
        try {
        i.open();
        while(i.hasNext()) {
            t = i.next();
            for(int k = 0; k < td_len; k++) {
                f = t.getField(k);
                type = td.getFieldType(k);
                if(f != null && type == Type.INT_TYPE) {
                    mins[k] = Math.min(mins[k], ((IntField) f).getValue());
                    maxes[k] = Math.max(maxes[k], ((IntField) f).getValue());
                } else {
                    str_fields[k] = ((StringField) f).getValue();
                }
            }
            pid = t.getRecordId().getPageId();
            pid_set.add(pid);
            tuple_count++;
        }

        //initialize histograms
        for(int k = 0; k < td_len; k++) {
            type = td.getFieldType(k);
            if(type == Type.INT_TYPE) {
                histograms[k] = new IntHistogram(NUM_HIST_BINS, mins[k], maxes[k]);
            } else if(type == Type.STRING_TYPE) {
                histograms[k] = new StringHistogram(NUM_HIST_BINS);
            } else {
                throw new RuntimeException("Unknown field type found!");
            }
        }

        i.rewind();
        while(i.hasNext()) {
            t = i.next();
            for(int k = 0; k < td_len; k++) {
                type = td.getFieldType(k);
                f = t.getField(k);
                if(f != null) {
                    if(type == Type.INT_TYPE) {
                        ((IntHistogram) histograms[k])
                        .addValue(((IntField) f).getValue());
                    } else if(type == Type.STRING_TYPE) {
                        ((StringHistogram) histograms[k])
                        .addValue(((StringField) f).getValue());
                    } else {
                        throw new RuntimeException("Unknown field type found!");
                    }
                }
            }
        }
        } catch(DbException e) {
            System.err.println("See TableStats()");
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            System.err.println("See TableStats()");
            e.printStackTrace();
        }
        i.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return page_io_cost * pid_set.size();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * tuple_count);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        double total = 0.0;
        int n = 0;
        Type type = td.getFieldType(field);
        for(int i = mins[field]; i <= maxes[field]; i++) {
            if(type == Type.INT_TYPE) {
              total += estimateSelectivity(field, op, new IntField(i));
            } else {
              total += estimateSelectivity(field, op, 
                       new StringField(str_fields[i], str_fields[i].length()));
            }
            n++;
        }
        return total / n;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = td.getFieldType(field);
        if(type == Type.INT_TYPE) {
            return ((IntHistogram) histograms[field])
                   .estimateSelectivity(op, ((IntField) constant).getValue());
        } else if(type == Type.STRING_TYPE) {
            return ((StringHistogram) histograms[field])
                   .estimateSelectivity(op, ((StringField) constant).getValue());
        } else {
            throw new RuntimeException("Unknown field type found!");
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tuple_count;
    }

}
