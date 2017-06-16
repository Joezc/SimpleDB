package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private TupleDesc td;
    private ArrayList<Tuple> tuples;
    private boolean isGrouped;

    private ArrayList<Integer> avgNum;

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
        this.avgNum = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
                        int newVal = this.update(tmp, tup);
                        tmp.setField(1, new IntField(newVal));

                        this.avgNum.set(i, this.avgNum.get(i)+1);
                    }
                }
                if (!flag) {
                    Tuple newTuple = new Tuple(this.td);
                    newTuple.setField(0, tup.getField(this.gbfield));
                    if (op.equals(Op.COUNT)) {
                        newTuple.setField(1, new IntField(1));
                    } else {
                        newTuple.setField(1, tup.getField(this.afield));
                    }
                    this.tuples.add(newTuple);
                    this.avgNum.add(new Integer(1));
                }
            } else {
                if (this.tuples.size() == 0) {
                    Tuple tmp = new Tuple(this.td);
                    if (op.equals(Op.COUNT)) {
                        tmp.setField(0, new IntField(1));
                    } else {
                        tmp.setField(0, tup.getField(this.afield));
                    }
                    this.tuples.add(tmp);
                    this.avgNum.add(new Integer(1));
                } else {
                    Tuple tmp = this.tuples.get(0);
                    int newVal = this.update(tmp, tup);
                    tmp.setField(0, new IntField(newVal));
                    this.tuples.set(0, tmp);
                    this.avgNum.set(0, this.avgNum.get(0)+1);
                }
            }
        }catch (Exception e) {

        }
    }

    private int update(Tuple tmp, Tuple tup) {
        int destField;
        if (isGrouped) {
            destField = 1;
        } else {
            destField = 0;
        }
        int tmpVal = ((IntField)tmp.getField(destField)).getValue();
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
        boolean isAvg = this.op.equals(Op.AVG);
        ArrayList<Tuple> res;
        if (isAvg) {
            res = new ArrayList<>();
            for (int i=0; i<this.tuples.size(); i++) {
                Tuple source = this.tuples.get(i);
                Tuple t = new Tuple(this.td);
                if (isGrouped) {
                    t.setField(0, source.getField(0));
                    int avg = ((IntField)source.getField(1)).getValue()/this.avgNum.get(i);
                    t.setField(1, new IntField(avg));
                } else {
                    int avg = ((IntField)source.getField(0)).getValue()/this.avgNum.get(i);
                    t.setField(0, new IntField(avg));
                }
                res.add(t);
            }
        } else {
            res = this.tuples;
        }
        return new TupleIterator(this.td, res);
    }

}
