package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    private int numPages;
    private LinkedHashMap<PageId, Page> pageMap;

    private HashMap<PageId, Set<TransactionId>> pageReadLocks;
    private HashMap<PageId, TransactionId> pageWriteLocks;

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new LinkedHashMap<>();

        this.pageReadLocks = new HashMap<>();
        this.pageWriteLocks = new HashMap<>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        while (!grantLock(tid, pid, perm)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Page res;
        if (pageMap.containsKey(pid)) {
            res = pageMap.get(pid);
            // move the page to the last
            pageMap.remove(pid);
            pageMap.put(pid, res);
        } else {
            if (pageMap.size() >= numPages) {
                this.evictPage();
            }
            res = Database.getCatalog()
                    .getDbFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, res);
        }
        return res;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        Set<TransactionId> rtid = pageReadLocks.get(pid);
        TransactionId wtid = pageWriteLocks.get(pid);
        if (rtid != null && rtid.contains(tid)) {
            rtid.remove(tid);
            pageReadLocks.replace(pid, rtid);
        }
        if (wtid != null && wtid.equals(tid)) {
            pageWriteLocks.remove(pid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        Set<TransactionId> rtid = pageReadLocks.get(p);
        TransactionId wtid = pageWriteLocks.get(p);
        return rtid.contains(tid) || wtid.equals(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        for (Map.Entry<PageId, Page> entry: this.pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            if ((pageReadLocks.containsKey(pid) && pageReadLocks.get(pid).contains(tid)) ||
                    (pageWriteLocks.containsKey(pid) && pageWriteLocks.get(pid).equals(tid))) {
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    if (commit) {
                        flushPage(pid);
                    } else {
                        pageMap.replace(pid, page.getBeforeImage());
                    }
                }
                releasePage(tid, pid);
            }
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbf = Database.getCatalog().getDbFile(tableId);
        HeapFile hf = (HeapFile) dbf;
        ArrayList<Page> affectedPages = hf.insertTuple(tid, t);
        for (Page p: affectedPages) {
            p.markDirty(true, tid);
//            pageMap.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        DbFile dbf = Database.getCatalog().getDbFile(
                t.getRecordId().getPageId().getTableId());
        HeapFile hf = (HeapFile) dbf;
        Page affectedPage = hf.deleteTuple(tid, t);
        HeapPage hp = (HeapPage)affectedPage;
        hp.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: this.pageMap.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        pageReadLocks.remove(pid);
        pageWriteLocks.remove(pid);
        this.pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p = this.pageMap.get(pid);
        int tableid = pid.getTableId();
        HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableid);
        hf.writePage(p);
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, Page> entry: this.pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            if ((pageReadLocks.containsKey(pid) && pageReadLocks.get(pid).contains(tid)) ||
                    (pageWriteLocks.containsKey(pid) && pageWriteLocks.get(pid).equals(tid))) {
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    flushPage(pid);
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        PageId evicted = null;
        Iterator<PageId> it = this.pageMap.keySet().iterator();
        boolean isFind = false;
        while (it.hasNext()) {
            evicted = it.next();
            if (pageMap.get(evicted).isDirty() == null) {
                isFind = true;
                break;
            }
        }
        if (!isFind) {
            throw new DbException("all pages in buffer pool are dirty");
        }
        discardPage(evicted);
    }

    private synchronized boolean grantLock(TransactionId tid,
                                           PageId pid, Permissions perm) {
        Set<TransactionId> rtid = pageReadLocks.get(pid);
        TransactionId wtid = pageWriteLocks.get(pid);
        if (perm.equals(Permissions.READ_ONLY)) {
            if (wtid == null || wtid.equals(tid)) {
                if (rtid == null) {
                    rtid = new HashSet<>();
                }
                rtid.add(tid);
                pageReadLocks.put(pid, rtid);
                return true;
            } else {
                return false;
            }
        } else {
            if (wtid != null) {
                return wtid.equals(tid);
            } else {
                if (rtid != null && rtid.size() > 0) {
                    if ((rtid.size() == 1) && rtid.contains(tid)) {
                        rtid.remove(tid);
                        pageReadLocks.put(pid, rtid);
                        pageWriteLocks.put(pid, tid);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    pageWriteLocks.put(pid, tid);
                    return true;
                }
            }
        }
    }
}
