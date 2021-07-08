package com.aurora.batch;

import com.aurora.connector.KuduTableInfo;
import com.aurora.connector.failure.DefaultKuduFailureHandler;
import com.aurora.connector.failure.KuduFailureHandler;
import com.aurora.connector.writer.KuduOperationMapper;
import com.aurora.connector.writer.KuduWriter;
import com.aurora.connector.writer.KuduWriterConfig;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description  写入kudu的写入格式
 * Output format for writing data into a Kudu table (defined by the provided {@link }) in both batch
 * and stream programs.
 * @date 2021-07-07
 */
@PublicEvolving
public class KuduOutputFormat<IN> extends RichOutputFormat<IN> implements CheckpointedFunction {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /** kudu table新型 */
    private final KuduTableInfo tableInfo;
    /** kudu 写入配置 */
    private final KuduWriterConfig writerConfig;
    /** kudu 失败处理器 */
    private final KuduFailureHandler failureHandler;
    /** kudu 操作映射 */
    private final KuduOperationMapper<IN> opsMapper;
    /** kudu 写入器 */
    private transient KuduWriter kuduWriter;

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper) {
        this(writerConfig, tableInfo, opsMapper, new DefaultKuduFailureHandler());
    }

    public KuduOutputFormat(KuduWriterConfig writerConfig, KuduTableInfo tableInfo, KuduOperationMapper<IN> opsMapper, KuduFailureHandler failureHandler) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo could not be null");
        this.writerConfig = checkNotNull(writerConfig, "config could not be null");
        this.opsMapper = checkNotNull(opsMapper, "opsMapper could not be null");
        this.failureHandler = checkNotNull(failureHandler, "failureHandler could not be null");
    }

    @Override
    public void configure(Configuration parameters) {
    }

    /**
     * @param taskNumber 并行的实例数
     * @param numTasks 并行任务数
     * @throws IOException
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        kuduWriter = new KuduWriter(tableInfo, writerConfig, opsMapper, failureHandler);
    }

    @Override
    public void writeRecord(IN row) throws IOException {
        // 序列化
        kuduWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        if (kuduWriter != null) {
            kuduWriter.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 做checkpoint
        kuduWriter.flushAndCheckErrors();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}


