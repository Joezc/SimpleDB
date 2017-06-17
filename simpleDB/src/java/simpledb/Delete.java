package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private DbIterator child;
    private final TupleDesc td;
    private boolean fetched;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NumOfDeletedTuples"});
        this.fetched = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                    Database.getBufferPool().deleteTuple(this.tid, tmp);
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
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
