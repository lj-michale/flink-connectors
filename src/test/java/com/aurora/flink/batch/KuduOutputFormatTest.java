package com.aurora.flink.batch;


import com.aurora.flink.connector.KuduTableInfo;
import com.aurora.flink.connector.KuduTestBase;
import com.aurora.flink.connector.writer.AbstractSingleOperationMapper;
import com.aurora.flink.connector.writer.KuduWriterConfig;
import com.aurora.flink.connector.writer.RowOperationMapper;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
class KuduOutputFormatTest extends KuduTestBase {

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(null, tableInfo, null));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduOutputFormat<>(writerConfig, null, null));
    }

    @Test
    void testNotTableExist() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), false);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters(masterAddresses).build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));
        Assertions.assertThrows(RuntimeException.class, () -> outputFormat.open(0, 1));
    }

    @Test
    void testOutputWithStrongConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setStrongConsistency()
                .build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        outputFormat.open(0, 1);

        for (Row kuduRow : booksDataRow()) {
            outputFormat.writeRecord(kuduRow);
        }
        outputFormat.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);

        cleanDatabase(tableInfo);
    }

    @Test
    void testOutputWithEventualConsistency() throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();

        KuduTableInfo tableInfo = booksTableInfo(UUID.randomUUID().toString(), true);
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setEventualConsistency()
                .build();
        KuduOutputFormat<Row> outputFormat = new KuduOutputFormat<>(writerConfig, tableInfo, new RowOperationMapper(KuduTestBase.columns, AbstractSingleOperationMapper.KuduOperation.INSERT));

        outputFormat.open(0, 1);

        for (Row kuduRow : booksDataRow()) {
            outputFormat.writeRecord(kuduRow);
        }

        // sleep to allow eventual consistency to finish
        Thread.sleep(1000);

        outputFormat.close();

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());
        kuduRowsTest(rows);

        cleanDatabase(tableInfo);
    }

}