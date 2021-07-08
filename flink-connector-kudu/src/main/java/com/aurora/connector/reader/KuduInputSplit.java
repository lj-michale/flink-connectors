package com.aurora.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.LocatableInputSplit;

/**
 * @author lj.michale
 * @description  kudu输入 split
 * @date 2021-07-07
 */
@Internal
public class KuduInputSplit extends LocatableInputSplit {

    private byte[] scanToken;

    /**
     * Creates a new KuduInputSplit
     *
     * @param splitNumber the number of the input split
     * @param hostnames   The names of the hosts storing the data this input split refers to.
     */
    public KuduInputSplit(byte[] scanToken, final int splitNumber, final String[] hostnames) {
        super(splitNumber, hostnames);

        this.scanToken = scanToken;
    }

    public byte[] getScanToken() {
        return scanToken;
    }
}