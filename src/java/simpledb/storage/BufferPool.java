package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    // 页面的最大数量
    private final int numPages;
    // 储存的页面
    // key 为 PageId
    private final ConcurrentHashMap<PageId, LinkedNode> pageStore;

    // 页面的访问顺序
    private static class LinkedNode {
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;

        public LinkedNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }

    // 头节点
    LinkedNode head;
    // 尾节点
    LinkedNode tail;

    private void addToHead(LinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void remove(LinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(LinkedNode node) {
        remove(node);
        addToHead(node);
    }

    private LinkedNode removeTail() {
        LinkedNode node = tail.prev;
        remove(node);
        return node;
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<>();
        head = new LinkedNode(new HeapPageId(-1, -1), null);
        tail = new LinkedNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // 如果缓存池中没有
        if (!pageStore.containsKey(pid)) {
            // 获取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // 是否超过大小
            if (pageStore.size() >= numPages) {
                // 淘汰 (后面的 lab 书写)
                eviction();
            }
            LinkedNode node = new LinkedNode(pid, page);
            // 放入缓存
            pageStore.put(pid, node);
            // 插入头节点
            addToHead(node);
        }
        // 移动到头部
        moveToHead(pageStore.get(pid));
        // 从 缓存池 中获取
        return pageStore.get(pid).page;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // 获取 数据库文件 DBfile
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        // 将页面刷新到缓存中
        updateBufferPollforInsert(heapFile.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // 查询所属表对应的文件
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        // 将页面刷新到缓存中
        updateBufferPoll(heapFile.deleteTuple(tid, t), tid);
    }

    /**
     * 更新缓存
     * 
     * @param pageList 需要更新的页面
     * @param tid      事务id
     */
    private void updateBufferPoll(List<Page> pageList, TransactionId tid) {
        for (Page page : pageList) {
            page.markDirty(true, tid);
            // 如果缓存池已满，执行淘汰策略
            if (pageStore.size() > numPages) {
                eviction();
            }
            // 获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候就已经放入缓存了
            LinkedNode node = pageStore.get(page.getId());
            // 更新新的页内容
            node.page = page;
            // 更新到缓存
            pageStore.put(page.getId(), node);
        }
    }
    
    private void updateBufferPollforInsert(List<Page> pageList, TransactionId tid) {
        for (Page page : pageList) {
            page.markDirty(true, tid);
            // 如果缓存池已满，执行淘汰策略
            if (pageStore.size() > numPages) {
                eviction();
            }
            // 获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候就已经放入缓存了
            LinkedNode node = pageStore.get(page.getId());
            // 更新新的页内容
            if (node == null) {
                node = new LinkedNode(page.getId(), page);
                // 放入缓存
                pageStore.put(page.getId(), node);
                // 插入头节点
                addToHead(node);
            }
            node.page = page;
            // 更新到缓存
            pageStore.put(page.getId(), node);
        }
    }

    /**
     * 淘汰策略
     * 使用 LRU 算法进行淘汰最近最久未使用
     */
    private void eviction() {
        // 淘汰尾部节点
        LinkedNode node = removeTail();
        try {
            flushPage(node.pageId);
        } catch (IOException e) {
            System.out.println("[simpledb] bufferpool: eviction IO error");
        }
        // 移除缓存中的记录
        pageStore.remove(node.pageId);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // not necessary for lab1
        for (PageId pageId : pageStore.keySet()) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) throws IOException {
        // 删除使用记录
        remove(pageStore.get(pid));
        try {
            flushPage(pid);
        } catch (IOException e) {
            System.out.println("[simpledb] bufferpool: eviction IO error");
        }
        pageStore.remove(pid);// 删除缓存
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // not necessary for lab1
        Page page = pageStore.get(pid).page;
        // 如果是是脏页
        if (page.isDirty() != null) {
            // 写入脏页
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            // 移除脏页标签 和 事务标签
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }
}
