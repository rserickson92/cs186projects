package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;
    private int max;
    private int range;
    private int ntups;
    private int[] hist;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	  // some code goes here
        hist = new int[buckets];
        this.min = min;
        this.max = max;
        range = (int) Math.ceil((max - min + 1) / ((double) buckets));
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
      if(v < min || v > max) { return; }
      hist[(v - min) / range]++;
      ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        boolean has_eq = op == Predicate.Op.EQUALS ||
                         op == Predicate.Op.GREATER_THAN_OR_EQ ||
                         op == Predicate.Op.LESS_THAN_OR_EQ ||
                         op == Predicate.Op.LIKE,
                has_lt = op == Predicate.Op.LESS_THAN_OR_EQ ||
                         op == Predicate.Op.LESS_THAN,
                has_gt = op == Predicate.Op.GREATER_THAN_OR_EQ ||
                         op == Predicate.Op.GREATER_THAN,
                neq    = op == Predicate.Op.NOT_EQUALS;
        
        //special cases for values outside of the interval [min, max]
        if(v < min) { return has_gt || neq ? 1.0 : 0.0; }
        if(v > max) { return has_lt || neq ? 1.0 : 0.0; }

        int bucket = (v - min) / range;
        int h = hist[bucket],
            w = range,
            b_left = min + bucket*range,
            b_right = min + (bucket+1)*range - 1;
        double sel = 0.0;
        if(neq) {
            return 1.0 - ((double)h) / w / ntups;
        }
        if(has_eq) {
            sel += ((double)h) / w / ntups;
        } 
        if(has_gt) {
            sel += h * (b_right - v) / ((double) w) / ntups;
            for(int i = bucket+1; i < hist.length; i++) {
                sel += hist[i] / ((double) ntups);
            }
        } 
        if(has_lt) {
            sel += h * (b_left + v) / ((double) w) / ntups;
            for(int i = bucket-1; i >= 0; i--) {
                sel += hist[i] / ((double) ntups);
            }
        }
        return sel;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        Predicate.Op[] ops = new Predicate.Op[] {Predicate.Op.EQUALS, 
                                                 Predicate.Op.GREATER_THAN, 
                                                 Predicate.Op.LESS_THAN, 
                                                 Predicate.Op.LESS_THAN_OR_EQ, 
                                                 Predicate.Op.GREATER_THAN_OR_EQ, 
                                                 Predicate.Op.LIKE, 
                                                 Predicate.Op.NOT_EQUALS};
        int n = 0;
        double total = 0.0;
        for(Predicate.Op op : ops) {
            for(int i = min; i <= max; i++) {
                total += estimateSelectivity(op, i);
                n++;
            }
        }
        return total / n;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        StringBuffer str = new StringBuffer();
        for(int i = 0; i < hist.length; i++) {
            str.append((min+i*range) + "," + (min+(i+1)*range-1) + ": ");
            for(int j = 0; j < hist[i]; j++) {
                str.append("=");
            }
            str.append("|\n");
        }
        str.append("width: ");
        str.append(range);
        return str.toString();
    }
}
