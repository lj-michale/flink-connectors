package com.aurora.flink.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.ColumnSchema;
import java.io.Serializable;
import java.util.List;

/**
 * 工厂为了创建"ColumnSchema"当一个表不存在在kudu的时候使用。
 * Factory for creating {@link ColumnSchema}s to be used when creating a table that
 * does not currently exist in Kudu. Usable through {@link KuduTableInfo#createTableIfNotExists}.
 *
 * <p> This factory implementation must be Serializable as it will be used directly in the Flink sources
 * and sinks.
 */
@PublicEvolving
public interface ColumnSchemasFactory extends Serializable {

    /**
     * Creates the columns of the Kudu table that will be used during the createTable operation.
     * 创建将在createTable操作期间使用的Kudu表的列。
     * @return List of columns. 列的集合
     */
    List<ColumnSchema> getColumnSchemas();

}