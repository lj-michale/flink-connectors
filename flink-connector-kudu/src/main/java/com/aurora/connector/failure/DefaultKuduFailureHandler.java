package com.aurora.connector.failure;

import org.apache.kudu.client.RowError;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lj.michale
 * @description 默认 - 场处理器
 * @date 2021-07-07
 */
public class DefaultKuduFailureHandler implements KuduFailureHandler {
    @Override
    public void onFailure(List<RowError> failure) throws IOException {
        // 将异常进行打印并且按照","分隔，然后跑出IOException异常
        String errors = failure.stream()
                .map(error -> error.toString() + System.lineSeparator())
                .collect(Collectors.joining());
        throw new IOException("Error while sending value. \n " + errors);
    }
}