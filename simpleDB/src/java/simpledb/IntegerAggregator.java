package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private TupleDesc td;
    private ArrayList<Tuple> tuples;
    private boolean isGrouped;

    private ArrayList<Integer> avgNum;
    private ArrayList<Integer> avgReminder;

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
        if (gbfield == Aggregator.NO_GROUPING) {
            isGrouped = false;
            Type[] typeAr = {Type.INT_TYPE};
            String[] fieldAr = {"aggregateVal"};
            this.td = new TupleDesc(typeAr, fieldAr);
        } else {
            isGrouped = true;
            Type[] typeAr = {gbfieldtype, Type.INT_TYPE};
            String[] fieldAr = {"groupVal", "aggregateVal"};
            this.td = new TupleDesc(typeAr, fieldAr);
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.tuples = new ArrayList<>();


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
        try {
            if (this.isGrouped) {
                DbIterator i = this.iterator();
                boolean flag = false;
                i.open();
                while (i.hasNext()) {
                    Tuple tmp = i.next();
                    if (tmp.getField(0).compare(Predicate.Op.EQUALS,
                            tup.getField(this.gbfield))) {
                        flag = true;
                        int newVal = this.update(tmp, tup);
                        tmp.setField(1, new IntField(newVal));
                    }
                }
                if (!flag) {
                    Tuple newTuple = new Tuple(this.td);
                    newTuple.setField(0, tup.getField(this.gbfield));
                    if (this.op.equals(Op.AVG)) {
                        newTuple.setField(1, new IntField(1));
                    } else {
                        newTuple.setField(1, tup.getField(this.afield));
                    }
                    this.tuples.add(newTuple);
                }
            } else {
                DbIterator i = this.iterator();
                i.open();
                Tuple tmp = i.next();
                int newVal = this.update(tmp, tup);
                tmp.setField(0, new IntField(newVal));
            }
        }catch (Exception e) {

        }
    }

    private int update(Tuple tmp, Tuple tup) {
        int tmpVal = ((IntField)tmp.getField(this.afield)).getValue();
        int tupVal = ((IntField)tup.getField(this.afield)).getValue();
        switch (this.op) {
            case MIN:
                tmpVal = Math.min(tmpVal, tupVal);
                break;
            case MAX:
                tmpVal = Math.max(tmpVal, tupVal);
                break;
            case SUM:
                tmpVal += tupVal;
                break;
            case AVG:
                tmpVal += tupVal;
                break;
            case COUNT:
                tmpVal++;
                break;
        }
        return tmpVal;
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
        return new TupleIterator(this.td, this.tuples);
    }

}
