package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TupleDesc td;
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private boolean fetched;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NumOfInsertedTuples"});
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int cnt;
        try {
            cnt = 0;
            if (this.fetched) {
                return null;
            } else {
                this.fetched = true;
                while (child.hasNext()) {
                    Tuple tmp = child.next();
                    try {
                        Database.getBufferPool().insertTuple(this.tid, this.tableid,tmp);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    cnt++;
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
            return null;
        }
        IntField intf = new IntField(cnt);
        Tuple res = new Tuple(this.td);
        res.setField(0, intf);
        return res;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
