package com.aurora.connector.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author lj.michale
 * @description
 * kudu元素映射基础类
 * Base implementation for {@link KuduOperationMapper}s that have one-to-one input to
 * Kudu operation mapping. It requires a fixed table schema to be provided at construction
 * time and only requires users to implement a getter for a specific column index (relative
 * to the ones provided in the constructor).
 * <br>
 * Supports both fixed operation type per record by specifying the {@link KuduOperation} or a
 * custom implementation for creating the base {@link Operation} throwugh the
 * {@link #createBaseOperation(Object, KuduTable)} method.
 *
 * @param <T> Input type
 * @date 2021-07-07
 */
@PublicEvolving
public abstract class AbstractSingleOperationMapper<T> implements KuduOperationMapper<T> {

    protected final String[] columnNames;
    private final KuduOperation operation;

    protected AbstractSingleOperationMapper(String[] columnNames) {
        this(columnNames, null);
    }

    public AbstractSingleOperationMapper(String[] columnNames, KuduOperation operation) {
        this.columnNames = columnNames;
        this.operation = operation;
    }

    /**
     * Returns the object corresponding to the given column index.
     *
     * @param input Input element
     * @param i     Column index
     * @return Column value
     */
    public abstract Object getField(T input, int i);

    public Optional<Operation> createBaseOperation(T input, KuduTable table) {
        if (operation == null) {
            throw new UnsupportedOperationException("createBaseOperation must be overridden if no operation specified in constructor");
        }
        /**
         * 根据不同操作类型封装不同的kudu operation
         */
        switch (operation) {
            case INSERT:
                return Optional.of(table.newInsert());
            case UPDATE:
                return Optional.of(table.newUpdate());
            case UPSERT:
                return Optional.of(table.newUpsert());
            case DELETE:
                return Optional.of(table.newDelete());
            default:
                throw new RuntimeException("Unknown operation " + operation);
        }
    }

    @Override
    public List<Operation> createOperations(T input, KuduTable table) {
        // 创建Operation
        Optional<Operation> operationOpt = createBaseOperation(input, table);
        if (!operationOpt.isPresent()) {
            return Collections.emptyList();
        }

        Operation operation = operationOpt.get();
        // 获取操作行
        PartialRow partialRow = operation.getRow();

        // 调用kudu client元素api，将列名和列值放入
        for (int i = 0; i < columnNames.length; i++) {
            partialRow.addObject(columnNames[i], getField(input, i));
        }

        // 返回Operation集合
        return Collections.singletonList(operation);
    }

    public enum KuduOperation {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }
}