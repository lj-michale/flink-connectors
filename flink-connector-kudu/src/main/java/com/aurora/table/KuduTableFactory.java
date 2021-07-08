package com.aurora.table;

import com.aurora.connector.KuduTableInfo;
import com.aurora.connector.writer.KuduWriterConfig;
import com.aurora.connector.reader.KuduReaderConfig;
import com.aurora.table.lookup.KuduLookupOptions;
import com.aurora.table.utils.KuduTableUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.Rowtime.*;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-07
 */
public class KuduTableFactory implements TableSourceFactory<Row>, TableSinkFactory<Tuple2<Boolean, Row>> {

    /**
     * Flink SQL的TableProperties中需要配置的json的key
     */
    public static final String KUDU_TABLE = "kudu.table";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String KUDU_HASH_COLS = "kudu.hash-columns";
    public static final String KUDU_HASH_PARTITION_NUMS = "kudu.hash-partition-nums";
    public static final String KUDU_RANGE_PARTITION_RULE = "kudu.range-partition-rule";
    public static final String KUDU_PRIMARY_KEY_COLS = "kudu.primary-key-columns";
    public static final String KUDU_REPLICAS = "kudu.replicas";
    public static final String KUDU_SCAN_ROW_SIZE = "kudu.scan.row-size";
    public static final String KUDU = "kudu";


    public static final String KUDU_LOOKUP_CACHE_MAX_ROWS = "kudu.lookup.cache.max-rows";
    public static final String KUDU_LOOKUP_CACHE_TTL = "kudu.lookup.cache.ttl";
    public static final String KUDU_LOOKUP_MAX_RETRIES = "kudu.lookup.max-retries";


    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, KUDU);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(KUDU_TABLE);
        properties.add(KUDU_MASTERS);
        properties.add(KUDU_HASH_COLS);
        properties.add(KUDU_PRIMARY_KEY_COLS);
        properties.add(KUDU_HASH_PARTITION_NUMS);
        properties.add(KUDU_RANGE_PARTITION_RULE);
        properties.add(KUDU_SCAN_ROW_SIZE);

        //lookup
        properties.add(KUDU_LOOKUP_CACHE_MAX_ROWS);
        properties.add(KUDU_LOOKUP_CACHE_TTL);
        properties.add(KUDU_LOOKUP_MAX_RETRIES);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
        return properties;
    }

    private DescriptorProperties validateTable(CatalogTable table) {
        Map<String, String> properties = table.toProperties();
        checkNotNull(properties.get(KUDU_MASTERS), "Missing required property " + KUDU_MASTERS);

        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        return descriptorProperties;
    }

    @Override
    public KuduTableSource createTableSource(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSource(tableName, table.getSchema(), table.getProperties());
    }

    private KuduTableSource createTableSource(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS);
        // 默认为0，表示拉取最大的数据量
        int scanRowSize = Integer.parseInt(props.getOrDefault(KUDU_SCAN_ROW_SIZE, "0"));
        long kuduCacheMaxRows = Long.parseLong(props.getOrDefault(KUDU_LOOKUP_CACHE_MAX_ROWS, "-1"));
        long kuduCacheTtl = Long.parseLong(props.getOrDefault(KUDU_LOOKUP_CACHE_TTL, "-1"));
        // kudu重试次数，默认为3次
        int kuduMaxReties = Integer.parseInt(props.getOrDefault(KUDU_LOOKUP_MAX_RETRIES, "3"));

        // 构造kudu lookup options
        KuduLookupOptions kuduLookupOptions = KuduLookupOptions.Builder.options().withCacheMaxSize(kuduCacheMaxRows)
                .withCacheExpireMs(kuduCacheTtl)
                .withMaxRetryTimes(kuduMaxReties)
                .build();

        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses)
                .setRowLimit(scanRowSize);

        return new KuduTableSource(configBuilder, tableInfo, physicalSchema, null, null, kuduLookupOptions);
    }

    @Override
    public KuduTableSink createTableSink(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSink(tableName, table.getSchema(), table.getProperties());
    }

    private KuduTableSink createTableSink(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS);
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);

        return new KuduTableSink(configBuilder, tableInfo, physicalSchema);
    }
}