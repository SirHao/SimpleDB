package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    // 需要聚合的 tuples
    private OpIterator child;
    private final int afield;// 聚合字段
    private final int gfield;// 分组字段
    private Aggregator.Op aop;// 运算符

    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        // 判断是否分组
        Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);

        // 创建聚合器
        if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
        } else {
            this.aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        }

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        if (gfieldtype != null) {
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]),
                nameList.toArray(new String[nameList.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        if (gfield == -1) {
            return tupleDesc.getFieldName(0);
        }
        return tupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // 获取聚合后的迭代器
        opIterator = aggregator.iterator();
        // 查询
        opIterator.open();
        // 使父类状态保持一致
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(opIterator.hasNext()){
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void close() {
        super.close();
        child.close();
        opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
        Type gfieldtype = child.getTupleDesc().getFieldType(gfield);

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        // 加入分组后的字段
        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        // 加入聚合字段
        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }

        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }

}
