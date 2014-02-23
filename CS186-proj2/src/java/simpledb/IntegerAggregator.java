package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregate;
    private HashMap<Field, Integer> avg_hash;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new HashMap<Field, Integer>();
        this.avg_hash = (what == Op.AVG) ? new HashMap<Field, Integer>() : null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbf = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField af = (IntField) tup.getField(afield);
        Integer oldval = aggregate.get(gbf);
        if(what == Op.COUNT) {
            if(oldval == null) {
                aggregate.put(gbf, new Integer(1));
            } else {
                aggregate.put(gbf, new Integer(oldval.intValue() + 1));
            }
        } else if(oldval == null) {
            aggregate.put(gbf, new Integer(af.getValue()));
            if(what == Op.AVG) { avg_hash.put(gbf, new Integer(1)); }
        } else {
            if(what == Op.SUM) {
                aggregate.put(gbf, new Integer(oldval.intValue() + af.getValue()));
            } else if(what == Op.AVG) {
                aggregate.put(gbf, new Integer(oldval.intValue() + af.getValue()));
                Integer old_count = avg_hash.get(gbf);
                avg_hash.put(gbf, new Integer(old_count.intValue() + 1));
            } else if(what == Op.MIN) {
                aggregate.put(gbf, new Integer(Math.min(oldval, af.getValue())));
            } else if(what == Op.MAX) {
                aggregate.put(gbf, new Integer(Math.max(oldval, af.getValue())));
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td;
        Tuple t = null;
        int i;
        Field ifield;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        if(gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE},
                               new String[]{"aggregateVal"});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                               new String[]{"groupVal", "aggregateVal"});
        }

        for(Field f : aggregate.keySet()) {
            t = new Tuple(td);
            i = aggregate.get(f).intValue();
            if(what != Op.AVG) {
                ifield = new IntField(i);
            } else {
                ifield = new IntField(i / avg_hash.get(f));
            }
            if(td.numFields() == 1) { //no grouping
                t.setField(0, ifield);
            } else { //grouping
                t.setField(0, f);
                t.setField(1, ifield);
            }
            //if(what == Op.COUNT) System.err.println(what + " " + t);
            tuples.add(t);
        }
        //if(what == Op.COUNT) System.err.println(what + " " + tuples);
        return new TupleIterator(td, tuples);
    }

}
