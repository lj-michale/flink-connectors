package com.aurora.connector;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.CreateTableOptions;

import java.io.Serializable;

/**
 * Factory for creating {@link CreateTableOptions} to be used when creating a table that
 * does not currently exist in Kudu. Usable through {@link KuduTableInfo#createTableIfNotExists}.
 *
 * <p> This factory implementation must be Serializable as it will be used directly in the Flink sources
 * and sinks.
 */
@PublicEvolving
public interface CreateTableOptionsFactory extends Serializable {

    /**
     * 创建CreateTableOptions将在CREATETABLE操作过程中使用
     * 用于封装创建table的熟悉
     * Creates the {@link CreateTableOptions} that will be used during the createTable operation.
     *
     * @return CreateTableOptions for creating the table.
     */
    CreateTableOptions getCreateTableOptions();
}