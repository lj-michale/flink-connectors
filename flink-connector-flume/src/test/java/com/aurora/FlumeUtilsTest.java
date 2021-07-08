package com.aurora;

import org.apache.flume.api.RpcClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
public class FlumeUtilsTest extends FlumeServerBaseTest {

    private RpcClient client;

    @Test
    public void testGetRpcClient() {
        client = FlumeUtils.getRpcClient("default",getHost(), getPort(), 1);
        Assertions.assertNotNull(client);
    }

    @Test
    public void testDestroy() {
        FlumeUtils.destroy(client);
        Assertions.assertNull(client);
    }

}
