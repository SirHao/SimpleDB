package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * INSERTS TUPLES READ FROM THE CHILD OPERATOR INTO THE TABLEID SPECIFIED IN THE
 * CONSTRUCTOR
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    // 插入的元组 迭代器
    private OpIterator child;
    // 要插入的表位置
    private final int tableId;

    // 标志位，避免 fetchNext 无限往下取
    private boolean inserted;
    // 返回的 tuple (用于展示插入了多少的 tuples)
    private final TupleDesc tupleDesc;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())){
            throw new DbException("插入的类型错误");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"the number of inserted tuple"});
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
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
        // some code goes here
        // 还未插入
        if(!inserted){
            // 计算插入了多少行
            inserted = true;
            int count = 0;
            while (child.hasNext()){
                Tuple tuple = child.next();
                try{
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            // 返回插入的次数 所组成的元组
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
