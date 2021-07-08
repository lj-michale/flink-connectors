package com.aurora.flink.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

/**
 * @author lj.michale
 * @description kduu读取器迭代器
 * @date 2021-07-07
 */
@Internal
public class KuduReaderIterator {

    /**
     * kudu scanner
     */
    private KuduScanner scanner;
    /**
     * 行结果迭代器
     */
    private RowResultIterator rowIterator;

    public KuduReaderIterator(KuduScanner scanner) throws KuduException {
        this.scanner = scanner;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public Row next() {
        RowResult row = this.rowIterator.next();
        return toFlinkRow(row);
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }

    /**
     * 将kudu行转换为flink Row
     * @param row
     * @return
     */
    private Row toFlinkRow(RowResult row) {
        Schema schema = row.getColumnProjection();

        Row values = new Row(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            values.setField(pos, row.getObject(name));
        });
        return values;
    }

}