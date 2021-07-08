package com.aurora.connector.writer;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.io.Serializable;
import java.util.List;

/**
 * 封装的映射输入记录（一个数据流），以在捻执行的操作的逻辑。 通过允许返回操作的列表，我们能灵活的实现者提供更复杂的逻辑
 * Encapsulates the logic of mapping input records (of a DataStream) to operations
 * executed in Kudu. By allowing to return a list of operations we give flexibility
 * to the implementers to provide more sophisticated logic.
 *
 * @param <T> Type of the input data
 */
@PublicEvolving
public interface KuduOperationMapper<T> extends Serializable {

    /**
     * Create a list of operations to be executed by the {@link KuduWriter} for the
     * current input
     * 用于将当前input写入table中
     * @param input input element
     * @param table table for which the operations should be created
     * @return List of operations to be executed on the table
     */
    List<Operation> createOperations(T input, KuduTable table);

}