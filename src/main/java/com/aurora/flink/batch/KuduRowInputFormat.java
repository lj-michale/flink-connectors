package com.aurora.flink.batch;

import com.aurora.flink.connector.KuduFilterInfo;
import com.aurora.flink.connector.KuduTableInfo;
import com.aurora.flink.connector.reader.KuduInputSplit;
import com.aurora.flink.connector.reader.KuduReader;
import com.aurora.flink.connector.reader.KuduReaderConfig;
import com.aurora.flink.connector.reader.KuduReaderIterator;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description 用于读取kudu行输入格式
 * @date 2021-07-07
 */
@PublicEvolving
public class KuduRowInputFormat extends RichInputFormat<Row, KuduInputSplit> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduReaderConfig readerConfig;
    private final KuduTableInfo tableInfo;

    private List<KuduFilterInfo> tableFilters;
    private List<String> tableProjections;

    private boolean endReached;

    private transient KuduReader kuduReader;
    private transient KuduReaderIterator resultIterator;

    public KuduRowInputFormat(KuduReaderConfig readerConfig, KuduTableInfo tableInfo) {
        this(readerConfig, tableInfo, new ArrayList<>(), null);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, KuduTableInfo tableInfo, List<String> tableProjections) {
        this(readerConfig, tableInfo, new ArrayList<>(), tableProjections);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, KuduTableInfo tableInfo, List<KuduFilterInfo> tableFilters, List<String> tableProjections) {

        this.readerConfig = checkNotNull(readerConfig, "readerConfig could not be null");
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableFilters = checkNotNull(tableFilters, "tableFilters could not be null");
        this.tableProjections = tableProjections;

        this.endReached = false;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
        endReached = false;
        startKuduReader();

        resultIterator = kuduReader.scanner(split.getScanToken());
    }

    private void startKuduReader() throws IOException {
        if (kuduReader == null) {
            kuduReader = new KuduReader(tableInfo, readerConfig, tableFilters, tableProjections);
        }
    }

    @Override
    public void close() throws IOException {
        if (resultIterator != null) {
            try {
                resultIterator.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        if (kuduReader != null) {
            kuduReader.close();
            kuduReader = null;
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        startKuduReader();
        return kuduReader.createInputSplits(minNumSplits);
    }

    @Override
    public boolean reachedEnd() {
        return endReached;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        // check that current iterator has next rows
        if (this.resultIterator.hasNext()) {
            return resultIterator.next();
        } else {
            endReached = true;
            return null;
        }
    }

}
