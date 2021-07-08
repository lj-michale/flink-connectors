package com.aurora.flink.common.handler;

import com.aurora.flink.common.config.FlinkJedisConfigBase;
import java.util.Map;

/**
 * @author lj.michale
 * @description handler to create flink jedis config.
 * @date 2021-07-08
 */
public interface FlinkJedisConfigHandler extends RedisHandler  {

    /**
     * create flink jedis config use sepecified properties.
     * @param properties used to create flink jedis config
     * @return flink jedis config
     */
    FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties);

}