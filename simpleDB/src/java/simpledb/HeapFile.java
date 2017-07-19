package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile rf = new RandomAccessFile(f, "r");
            int offset = pid.pageNumber() * Database.getBufferPool().PAGE_SIZE;
            byte[] b = new byte[Database.getBufferPool().PAGE_SIZE];
            rf.seek(offset);
            rf.read(b, 0, Database.getBufferPool().PAGE_SIZE);
            rf.close();
            HeapPageId hpid = (HeapPageId) pid;
            return new HeapPage(hpid, b);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            RandomAccessFile rf = new RandomAccessFile(f, "rws");
            PageId pid = page.getId();
            int pos = pid.pageNumber() * Database.getBufferPool().PAGE_SIZE;
            byte[] b = page.getPageData();
            rf.seek(pos);
            rf.write(b, 0, Database.getBufferPool().PAGE_SIZE);

            rf.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(f.length()/
                Database.getBufferPool().PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableID = this.getId();
        for (int i=0; i<this.numPages(); i++) {
            HeapPageId pid = new HeapPageId(tableID, i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(
                    tid, pid, Permissions.READ_WRITE);
            if (p.getNumEmptySlots() != 0) {
                p.insertTuple(t);
                ArrayList<Page> res = new ArrayList<>();
                res.add(p);
                return res;
            }
        }
        // insert a page
        HeapPageId pid = new HeapPageId(tableID, this.numPages());
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hp.insertTuple(t);
        ArrayList<Page> res = new ArrayList<>();
        res.add(hp);
        this.writePage(hp);
        return res;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        try {
            Page p = Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
            HeapPage hp = (HeapPage) p;
            hp.deleteTuple(t);
            return hp;
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
//            e.printStackTrace();
//            System.out.println();
            throw new TransactionAbortedException();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

