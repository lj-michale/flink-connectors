package com.aurora.table;

import com.aurora.connector.KuduTableInfo;
import com.aurora.table.utils.KuduTableUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.util.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description  用于读取和创建kudu表
 * @date 2021-07-07
 */
@PublicEvolving
public class KuduCatalog extends AbstractReadOnlyCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalog.class);
    // kuduTable工厂类
    private final KuduTableFactory tableFactory = new KuduTableFactory();
    private final String kuduMasters;
    private KuduClient kuduClient;

    /**
     * Create a new {@link KuduCatalog} with the specified catalog name and kudu master addresses.
     *
     * @param catalogName Name of the catalog (used by the table environment)
     * @param kuduMasters Connection address to Kudu
     */
    public KuduCatalog(String catalogName, String kuduMasters) {
        super(catalogName, EnvironmentSettings.DEFAULT_BUILTIN_DATABASE);
        this.kuduMasters = kuduMasters;
        this.kuduClient = createClient();
    }

    /**
     * Create a new {@link KuduCatalog} with the specified kudu master addresses.
     *
     * @param kuduMasters Connection address to Kudu
     */
    public KuduCatalog(String kuduMasters) {
        this("kudu", kuduMasters);
    }

    @Override
    public Optional<TableFactory> getTableFactory() {
        return Optional.of(getKuduTableFactory());
    }

    public KuduTableFactory getKuduTableFactory() {
        return tableFactory;
    }

    private KuduClient createClient() {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
        try {
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (KuduException e) {
            LOG.error("Error while closing kudu client", e);
        }
    }

    public ObjectPath getObjectPath(String tableName) {
        return new ObjectPath(getDefaultDatabase(), tableName);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            // 调用原生kudu 客户端
            return kuduClient.getTablesList().getTablesList();
        } catch (Throwable t) {
            throw new CatalogException("Could not list tables", t);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        checkNotNull(tablePath);
        try {
            return kuduClient.tableExists(tablePath.getObjectName());
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    /**
     * @param tablePath databaseName.tableName
     * @return
     * @throws TableNotExistException
     */
    @Override
    public CatalogTable getTable(ObjectPath tablePath) throws TableNotExistException {
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String tableName = tablePath.getObjectName();

        try {
            // 打开kudu table
            KuduTable kuduTable = kuduClient.openTable(tableName);
            // 将kudu table封装为flink catalogTable
            return new CatalogTableImpl(
                    // 将kudu schema转换为flink schema
                    KuduTableUtils.kuduToFlinkSchema(kuduTable.getSchema()),
                    // 构建table参数
                    createTableProperties(tableName, kuduTable.getSchema().getPrimaryKeyColumns()),
                    tableName);
        } catch (KuduException e) {
            throw new CatalogException(e);
        }
    }

    /**
     * 创建table属性
     *
     * @param tableName         表名
     * @param primaryKeyColumns 主键列
     * @return
     */
    protected Map<String, String> createTableProperties(String tableName, List<ColumnSchema> primaryKeyColumns) {
        Map<String, String> props = new HashMap<>();
        // kudu master
        props.put(KuduTableFactory.KUDU_MASTERS, kuduMasters);
        // 主键列拼接
        String primaryKeyNames = primaryKeyColumns.stream().map(ColumnSchema::getName).collect(Collectors.joining(","));
        // 主键列
        props.put(KuduTableFactory.KUDU_PRIMARY_KEY_COLS, primaryKeyNames);
        // 表名
        props.put(KuduTableFactory.KUDU_TABLE, tableName);
        return props;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.deleteTable(tableName);
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not delete table " + tableName, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException {
        String tableName = tablePath.getObjectName();
        try {
            if (tableExists(tablePath)) {
                kuduClient.alterTable(tableName, new AlterTableOptions().renameTable(newTableName));
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (KuduException e) {
            throw new CatalogException("Could not rename table " + tableName, e);
        }
    }

    /**
     * 创建table
     *
     * @param tableInfo      table信息包含tableName，CreateTableOptionsFactory、ColumnSchemasFactory等，用于创建Schema和CreateTableOptions
     * @param ignoreIfExists
     * @throws CatalogException
     * @throws TableAlreadyExistException
     */
    public void createTable(KuduTableInfo tableInfo, boolean ignoreIfExists) throws CatalogException, TableAlreadyExistException {
        ObjectPath path = getObjectPath(tableInfo.getName());
        if (tableExists(path)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(getName(), path);
            }
        }

        try {
            // 创建表
            kuduClient.createTable(tableInfo.getName(), tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        } catch (
                KuduException e) {
            throw new CatalogException("Could not create table " + tableInfo.getName(), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
        Map<String, String> tableProperties = table.getProperties();
        TableSchema tableSchema = table.getSchema();

        Set<String> optionalProperties = new HashSet<>(Arrays.asList(KuduTableFactory.KUDU_REPLICAS, KuduTableFactory.KUDU_HASH_PARTITION_NUMS, KuduTableFactory.KUDU_RANGE_PARTITION_RULE, KuduTableFactory.KUDU_HASH_COLS));
        Set<String> requiredProperties = new HashSet<>();

        if (!tableSchema.getPrimaryKey().isPresent()) {
            requiredProperties.add(KuduTableFactory.KUDU_PRIMARY_KEY_COLS);
        }

        if (!tableProperties.keySet().containsAll(requiredProperties)) {
            throw new CatalogException("Missing required property. The following properties must be provided: " +
                    requiredProperties.toString());
        }

        // 必须、可选参数合并
        Set<String> permittedProperties = Sets.union(requiredProperties, optionalProperties);
        // 校验tableProperties中是否包含全部参数
        if (!permittedProperties.containsAll(tableProperties.keySet())) {
            throw new CatalogException("Unpermitted properties were given. The following properties are allowed:" +
                    permittedProperties.toString());
        }

        String tableName = tablePath.getObjectName();

        // 构建底层KuduTableInfo
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, tableSchema, tableProperties);

        // 创建表
        createTable(tableInfo, ignoreIfExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Lists.newArrayList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

}