package com.aurora.flink.batch;


import com.aurora.flink.connector.KuduTableInfo;
import com.aurora.flink.connector.KuduTestBase;
import com.aurora.flink.connector.reader.KuduInputSplit;
import com.aurora.flink.connector.reader.KuduReaderConfig;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author lj.michale
 * @description
 *    单元测试：测试kudu连接器的写入功能
 * @date 2021-07-08
 */
class KuduInputFormatTest extends KuduTestBase {

    @Test
    void testInvalidKuduMaster() {
        KuduTableInfo tableInfo = booksTableInfo("books", false);
        Assertions.assertThrows(NullPointerException.class, () -> new KuduRowInputFormat(null, tableInfo));
    }

    @Test
    void testInvalidTableInfo() {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        Assertions.assertThrows(NullPointerException.class, () -> new KuduRowInputFormat(readerConfig, null));
    }

    @Test
    void testInputFormat() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);

        List<Row> rows = readRows(tableInfo);
        Assertions.assertEquals(5, rows.size());

        cleanDatabase(tableInfo);
    }

    @Test
    void testInputFormatWithProjection() throws Exception {
        KuduTableInfo tableInfo = booksTableInfo("books", true);
        setUpDatabase(tableInfo);

        List<Row> rows = readRows(tableInfo, "title", "id");
        Assertions.assertEquals(5, rows.size());

        for (Row row : rows) {
            Assertions.assertEquals(2, row.getArity());
        }

        cleanDatabase(tableInfo);
    }

    private List<Row> readRows(KuduTableInfo tableInfo, String... fieldProjection) throws Exception {
        String masterAddresses = harness.getMasterAddressesAsString();
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(masterAddresses).build();
        KuduRowInputFormat inputFormat = new KuduRowInputFormat(readerConfig, tableInfo, new ArrayList<>(),
                fieldProjection == null ? null : Arrays.asList(fieldProjection));

        KuduInputSplit[] splits = inputFormat.createInputSplits(1);
        List<Row> rows = new ArrayList<>();
        for (KuduInputSplit split : splits) {
            inputFormat.open(split);
            while (!inputFormat.reachedEnd()) {
                Row row = inputFormat.nextRecord(new Row(5));
                if (row != null) {
                    rows.add(row);
                }
            }
        }
        inputFormat.close();

        return rows;
    }
}
