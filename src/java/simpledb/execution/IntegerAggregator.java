package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    // 分组字段的序号（是一个字段，用于辨别是否是该类型）举例：group by 字段
    private final int gbfieldId;
    private final Type gbfieldtype;
    // 聚合字段的序号（是用于取新插入的值） 举例： sum(字段),min(字段)
    private int afield;
    // 操作符
    private Op what;

    private AggHandler aggHandler;

    private abstract class AggHandler {
        // 存储字段对应的聚合结果
        Map<Field, Integer> aggResult;

        // gbField 用于分组的字段， aggField 现阶段聚合结果
        abstract void handle(Field gbField, IntField aggField);

        public AggHandler() {
            aggResult = new HashMap<>();
        }

        public Map<Field, Integer> getAggResult() {
            return aggResult;
        }
    }
    private class CountHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            }
            else{
                aggResult.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + value);
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class MaxHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField,Math.max(aggResult.get(gbField), value));
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class MinHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField,Math.min(aggResult.get(gbField), value));
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class AvgHandler extends AggHandler{
        Map<Field, Integer> sum = new HashMap<>();
        Map<Field, Integer> count = new HashMap<>();
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            // 求和 + 计数
            if(sum.containsKey(gbField) && count.containsKey(gbField)){
                sum.put(gbField, sum.get(gbField) + value);
                count.put(gbField, count.get(gbField) + 1);
            }
            else{
                sum.put(gbField, value);
                count.put(gbField, 1);
            }
            aggResult.put(gbField, sum.get(gbField) / count.get(gbField));
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
       // some code goes here
       this.gbfieldId = gbfield;
       this.gbfieldtype = gbfieldtype;
       this.afield = afield;
       this.what = what;
       // 判读运算符号
       switch (what){
           case MIN:
               aggHandler = new MinHandler();
               break;
           case MAX:
               aggHandler = new MaxHandler();
               break;
           case AVG:
               aggHandler = new AvgHandler();
               break;
           case SUM:
               aggHandler = new SumHandler();
               break;
           case COUNT:
               aggHandler = new CountHandler();
               break;
           default:
               throw new IllegalArgumentException("聚合器不支持当前运算符");
       }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField afield = (IntField) tup.getField(this.afield);
        // 分组的字段
        Field groupfield = this.gbfieldId == NO_GROUPING ? null : tup.getField(this.gbfieldId);
        aggHandler.handle(groupfield, afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Map<Field, Integer> aggResult = aggHandler.getAggResult();
        // 构建 tuple 需要
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        // 储存结果
        List<Tuple> tuples = new ArrayList<>();
        // 如果没有分组
        if(gbfieldId == NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            // 获取结果字段
            IntField resultField = new IntField(aggResult.get(null));
            // 组合成行（临时行，不需要存储，只需要设置字段值）
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }
        else{
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            names = new String[]{"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            for(Field field: aggResult.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbfieldtype == Type.INT_TYPE){
                    IntField intField = (IntField) field;
                    tuple.setField(0, intField);
                }
                else{
                    StringField stringField = (StringField) field;
                    tuple.setField(0, stringField);
                }

                IntField resultField = new IntField(aggResult.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc ,tuples);
    }

}
