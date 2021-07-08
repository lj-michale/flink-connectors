package com.aurora.flink.connector.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.commons.lang3.builder.ToStringBuilder;
import java.io.Serializable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kudu.client.SessionConfiguration.FlushMode;

/**
 * @author lj.michale
 * @description Configuration used by {@link org.apache.flink.connectors.kudu.streaming.KuduSink} and {@link org.apache.flink.connectors.kudu.batch.KuduOutputFormat}.
 * Specifies connection and other necessary properties.
 * @date 2021-07-07
 */
@PublicEvolving
public class KuduWriterConfig implements Serializable {

    /**
     * kudu master server地址
     */
    private final String masters;
    /**
     * 会话刷新模式
     */
    private final FlushMode flushMode;

    private KuduWriterConfig(
            String masters,
            FlushMode flushMode) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flushMode", flushMode)
                .toString();
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {
        private String masters;
        private FlushMode flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setConsistency(FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }

        public Builder setEventualConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_BACKGROUND);
        }

        public Builder setStrongConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_SYNC);
        }

        public KuduWriterConfig build() {
            return new KuduWriterConfig(
                    masters,
                    flushMode);
        }
    }
}