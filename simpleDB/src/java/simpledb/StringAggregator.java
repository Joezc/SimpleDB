package simpledb;

import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private TupleDesc td;
    private ArrayList<Tuple> tuples;
    private boolean isGrouped;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Op Only supports COUNT");
        }
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
        this.tuples = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        try {
            if (this.isGrouped) {
                boolean flag = false;
                for (int i=0; i<this.tuples.size(); i++) {
                    Tuple tmp = this.tuples.get(i);
                    if (tmp.getField(0).compare(Predicate.Op.EQUALS,
                            tup.getField(this.gbfield))) {
                        flag = true;
                        int tmpVal = ((IntField)tmp.getField(this.afield)).getValue();
                        tmpVal++;
                        tmp.setField(1, new IntField(tmpVal));
                    }
                }
                if (!flag) {
                    Tuple newTuple = new Tuple(this.td);
                    newTuple.setField(0, tup.getField(this.gbfield));
                    newTuple.setField(1, new IntField(1));
                    this.tuples.add(newTuple);
                }
            } else {
                if (this.tuples.size() == 0) {
                    Tuple tmp = new Tuple(this.td);
                    tmp.setField(0, new IntField(1));
                    this.tuples.add(tmp);
                } else {
                    Tuple tmp = this.tuples.get(0);
                    int tmpVal = ((IntField)tmp.getField(this.afield)).getValue();
                    tmpVal++;
                    tmp.setField(1, new IntField(tmpVal));
                }
            }
        }catch (Exception e) {

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
        return new TupleIterator(this.td, this.tuples);
    }

}
