package com.aurora.flink.descriptor;

/**
 * @author lj.michale
 * @description redis validator for validate redis descriptor.
 * @date 2021-07-08
 */
public class RedisValidator {
    public static final String REDIS = "redis";
    public static final String REDIS_MODE = "redis-mode";
    public static final String REDIS_NODES = "cluster-nodes";
    public static final String REDIS_CLUSTER = "cluster";
    public static final String REDIS_SENTINEL = "sentinel";
    public static final String REDIS_COMMAND = "command";
    public static final String REDIS_MASTER_NAME = "master.name";
    public static final String SENTINELS_INFO = "sentinels.info";
    public static final String SENTINELS_PASSWORD = "sentinels.password";
    public static final String REDIS_KEY_TTL = "key.ttl";
}