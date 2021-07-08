package com.aurora.connector.writer;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

/**
 * @author lj.michale
 * @description Row操作映射
 * @date 2021-07-08
 */
@PublicEvolving
public class RowOperationMapper extends AbstractSingleOperationMapper<Row> {

    protected RowOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    public RowOperationMapper(String[] columnNames, KuduOperation operation) {
        super(columnNames, operation);
    }

    @Override
    public Object getField(Row input, int i) {
        return input.getField(i);
    }
}