package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 *
 *
 * HeapFileIterator is an implementation of a DbFileIterator that repreasents
 * a iterator iterate through through the tuples of each page in the HeapFile
 * Created by lzc on 17-6-12.
 */
public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile f;
    private int pgNum;
    private Iterator<Tuple> it;

    /**
     *
     * @param tid The transaction performing the update
     * @param f the heap file iteratored
     */
    public HeapFileIterator(TransactionId tid, HeapFile f) {
        this.tid = tid;
        this.f = f;
    }

    private Iterator<Tuple> getPageIterator(int pgNum)
            throws TransactionAbortedException, DbException {
        PageId pageId = new HeapPageId(f.getId(), pgNum);
        Page page = Database.getBufferPool().getPage(tid,
                pageId, Permissions.READ_ONLY);

        ArrayList<Tuple> tupleList = new ArrayList<Tuple>();

        HeapPage hp = (HeapPage)page;
        Iterator<Tuple> itr = hp.iterator();
        return itr;
    }

    // see DbFileIterator.java for javadocs
    @Override
    public void open() throws DbException, TransactionAbortedException {
        pgNum = 0;
        it = getPageIterator(pgNum);
    }

    // see DbFileIterator.java for javadocs
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (it == null) {
            return false;
        } else {
            if (it.hasNext()) {
                return true;
            } else {
                if (pgNum < f.numPages()-1) {
                    if (!getPageIterator(pgNum+1).hasNext()) {
                        return false;
                    } else {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    // see DbFileIterator.java for javadocs
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (it == null) {
            throw new NoSuchElementException("tuple if null");
        }

        if (it.hasNext()) {
            return it.next();
        } else {
            if (pgNum < f.numPages() - 1) {
                pgNum++;
                it = getPageIterator(pgNum);
                if (it.hasNext()) {
                    return it.next();
                } else {
                    throw new NoSuchElementException("No more tuples");
                }
            } else {
                throw new NoSuchElementException("No more tuples");
            }
        }
    }

    // see DbFileIterator.java for javadocs
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    // see DbFileIterator.java for javadocs
    @Override
    public void close() {
        it = null;
    }
}
