package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private int buckets;
    private int[] histogram;
    private int width;
    private int ntups;
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
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        this.histogram = new int[buckets];

        double range = (double) (max-min+1)/buckets;
        this.width = (int) Math.ceil(range);
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v == min) {
            histogram[0]++;
        } else if (v == max) {
            histogram[buckets-1]++;
        } else {
            int idx = (v - min) / width;
            histogram[idx]++;
        }
        this.ntups++;
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
        double res = -1.0;
        int idx = (v-min)/width;
        int left = idx * width + min;
        int right = idx * width + min + width -1;
        if (op.equals(Predicate.Op.EQUALS) || op.equals(Predicate.Op.LIKE) || op.equals(Predicate.Op.NOT_EQUALS)) {
            if (v < min || v > max) {
                res = 0.0;
            } else {
                res = (double)(histogram[idx] / width) / ntups;
            }
            if (op.equals(Predicate.Op.NOT_EQUALS)) {
                res = 1.0 - res;
            }
        } else {
            if (op.equals(Predicate.Op.GREATER_THAN) || (op.equals(Predicate.Op.GREATER_THAN_OR_EQ))) {
                if (v < min) {
                    res = 1.0;
                } else if (v >= max){
                    res = 0.0;
                } else{
                    int s = 0;
                    for (int i = idx + 1; i < histogram.length; i++) {
                        s += histogram[i];
                    }
                    res = (double)histogram[idx] / ntups * (right - v) / width + 1.0 * s / ntups;
                    if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                        res += (double)histogram[idx]/width/ntups;
                    }
                }
            }
            if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                if (v < min) {
                    res = 0.0;
                } else if (v >= max){
                    res = 1.0;
                } else{
                    int s = 0;
                    for (int i = 0; i < idx; i++) {
                        s += histogram[i];
                    }
                    res = (double)histogram[idx] / ntups * (v - left) / width + 1.0 * s / ntups;
                    if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                        res += (double)histogram[idx]/width/ntups;
                    }
                }
            }
        }
        return res;
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
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
