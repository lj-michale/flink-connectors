package com.aurora.connector.writer;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple;

@PublicEvolving
public class TupleOperationMapper<T extends Tuple> extends AbstractSingleOperationMapper<T> {

    protected TupleOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    public TupleOperationMapper(String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
    }

    @Override
    public Object getField(T input, int i) {
        return input.getField(i);
    }
}