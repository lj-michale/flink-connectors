package com.aurora.flink.common.config.handler;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.aurora.flink.common.config.FlinkJedisClusterConfig;
import com.aurora.flink.common.config.FlinkJedisConfigBase;
import com.aurora.flink.common.handler.FlinkJedisConfigHandler;
import org.apache.flink.util.Preconditions;

import static com.aurora.flink.descriptor.RedisValidator.*;

/**
 * @author lj.michale
 * @description jedis cluster config handler to find and create jedis cluster config use meta.
 * @date 2021-07-08
 */
public class FlinkJedisClusterConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        Preconditions.checkArgument(properties.containsKey(REDIS_NODES), "nodes should not be null in cluster mode");
        String nodesInfo = properties.get(REDIS_NODES);
        Set<InetSocketAddress> nodes = Arrays.asList(nodesInfo.split(",")).stream().map(r -> {
            String[] arr = r.split(":");
            return new InetSocketAddress(arr[0].trim(), Integer.parseInt(arr[1].trim()));
        }).collect(Collectors.toSet());
        return new FlinkJedisClusterConfig.Builder()
                .setNodes(nodes).build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(REDIS_MODE, REDIS_CLUSTER);
        return require;
    }

    public FlinkJedisClusterConfigHandler() {
    }
}