package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregate;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) { 
            throw new IllegalArgumentException("StringAggregator only supports COUNT"); 
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbf = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        Integer oldval = aggregate.get(gbf);
        if(oldval == null) {
            aggregate.put(gbf, new Integer(1));
        } else {
            aggregate.put(gbf, new Integer(oldval.intValue() + 1));
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td;
        Tuple t = null;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        String aggregateName = what + "aggregateVal"; 
        if(gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE},
                               new String[]{aggregateName});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                               new String[]{"groupVal", aggregateName});
        }

        for(Field f : aggregate.keySet()) {
            t = new Tuple(td);
            IntField ifield = new IntField(aggregate.get(f).intValue());
            if(td.numFields() == 1) { //no grouping
                t.setField(0, ifield);
            } else { //grouping
                t.setField(0, f);
                t.setField(1, ifield);
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
