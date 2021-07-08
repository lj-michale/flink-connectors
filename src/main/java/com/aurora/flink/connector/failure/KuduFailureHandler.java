package com.aurora.flink.connector.failure;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.RowError;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * @author lj.michale
 * @description  在KuduWriter尝试执行kudu操作的时候自定义错误结果处理逻辑
 * @date 2021-07-07
 */
public interface KuduFailureHandler extends Serializable {

    /**
     * 处理List<RowError>错误
     * Handle a failed {@link List<RowError>}.
     *
     * @param failure the cause of failure 错误的原因
     * @throws IOException if the sink should fail on this failure, the implementation should rethrow the throwable or a custom one
     */
    void onFailure(List<RowError> failure) throws IOException;

    /**
     * 处理类型转换一场，默认实现rethrows这个一场
     * Handle a ClassCastException. Default implementation rethrows the exception.
     *
     * @param e the cause of failure
     * @throws IOException if the casting failed
     */
    default void onTypeMismatch(ClassCastException e) throws IOException {
        throw new IOException("Class casting failed \n", e);
    }


}
