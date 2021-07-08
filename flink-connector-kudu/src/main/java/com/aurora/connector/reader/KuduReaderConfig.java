package com.aurora.connector.reader;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description 配置使用KuduRowInputFormat
 * 指定连接和其他必要的配置
 *
 * @date 2021-07-07
 */
@PublicEvolving
public class KuduReaderConfig implements Serializable {

    /**
     * kudu master配置
     */
    private final String masters;
    /**
     * kudu读取行数限制
     */
    private final int rowLimit;

    private KuduReaderConfig(
            String masters,
            int rowLimit) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.rowLimit = checkNotNull(rowLimit, "Kudu rowLimit cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("rowLimit", rowLimit)
                .toString();
    }

    /**
     * Builder for the {@link KuduReaderConfig}.
     */
    public static class Builder {
        private static final int DEFAULT_ROW_LIMIT = 0;

        private final String masters;
        private final int rowLimit;

        private Builder(String masters) {
            this(masters, DEFAULT_ROW_LIMIT);
        }

        private Builder(String masters, Integer rowLimit) {
            this.masters = masters;
            this.rowLimit = rowLimit;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setRowLimit(int rowLimit) {
            return new Builder(masters, rowLimit);
        }

        public KuduReaderConfig build() {
            return new KuduReaderConfig(
                    masters,
                    rowLimit);
        }
    }
}