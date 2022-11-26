package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // 文件的绝对路径，取hash。独一无二的id
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 表id
        int tableId = pid.getTableId();
        // 该表所处的页码
        int pgNo = pid.getPageNumber();
        // 随机访问,指针偏移访问
        RandomAccessFile f = null;
        try{
            // 读取当前文件
            f = new RandomAccessFile(file, "r");
            // 当前页号 * 每页的字节大小 是否超出文件的范围
            if((pgNo + 1) * BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
            }
            // 用于储存
            byte[] bytes = new byte[BufferPool.getPageSize()];
            // 指针偏移
            f.seek(pgNo * BufferPool.getPageSize());
            // 读取(返回读取的数量)
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            // 如果取出来少了，说明不存在
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
            }
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                // 关闭流
                assert f != null;
                f.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // 获取页面序号
        int pageId = page.getId().getPageNumber();
        // 不能超过最大页面数
        if(pageId > numPages()){
            throw new IllegalArgumentException();
        }
        // 创建写入工具
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        // 跳过前面的页面
        f.seek(pageId * BufferPool.getPageSize());
        // 写入数据
        f.write(page.getPageData());
        // 刷盘
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 文件长度 / 每页的字节数
        int res = (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
        return res;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        ArrayList<Page> list = new ArrayList<>();
        // 查询现有的页
        for (int pageNo = 0; pageNo < numPages(); pageNo++) {
            // 查询页
            HeapPageId pageId = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            // 看当前页是有 空闲空间
            if(page.getNumEmptySlots() != 0){
                page.insertTuple(t);
                list.add(page);
                return list;
            }
        }

        // 如果所有页都已经写满，就要新建新的页面来加入(记得开启 append = true 也就是增量增加)
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file, true));
        // 新建一个空的页
        byte[] emptyPage = HeapPage.createEmptyPageData();
        output.write(emptyPage);
        // close 前会调用 flush() 刷盘到文件
        output.close();

        // 创建新的页面
        HeapPageId pageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        // 找到相应的页
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        // 元组迭代器
        private Iterator<Tuple> iterator;
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // 获取第一页的全部元组
            whichPage = 0;
            iterator = getPageTuple(whichPage);
        }

        // 获取当前页的所有行
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
            // 在文件范围内
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                // 从缓存池中查询相应的页面 读权限
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // 如果迭代器为空
            if(iterator == null){
                return false;
            }
            // 如果已经遍历结束
            if(!iterator.hasNext()){
                // 是否还存在下一页，小于文件的最大页
                while(whichPage < (heapFile.numPages() - 1)){
                    whichPage++;
                    // 获取下一页
                    iterator = getPageTuple(whichPage);
                    if(iterator.hasNext()){
                        return iterator.hasNext();
                    }
                }
                // 所有元组获取完毕
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 如果没有元组了，抛出异常
            if(iterator == null || !iterator.hasNext()){
                throw new NoSuchElementException();
            }
            // 返回下一个元组
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 清除上一个迭代器
            close();
            // 重新开始
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}
