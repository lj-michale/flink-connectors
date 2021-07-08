package com.aurora.table;


import com.aurora.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.Optional;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-07
 */
@Internal
public class UpsertOperationMapper extends AbstractSingleOperationMapper<Tuple2<Boolean, Row>> {

    public UpsertOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    @Override
    public Object getField(Tuple2<Boolean, Row> input, int i) {
        return input.f1.getField(i);
    }

    @Override
    public Optional<Operation> createBaseOperation(Tuple2<Boolean, Row> input, KuduTable table) {
        return Optional.of(input.f0 ? table.newUpsert() : table.newDelete());
    }
}