package com.aurora;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */
public class FlumeServerBaseTest {

    private static final Integer EXPOSED_PORT = 44444;

    public GenericContainer<?> sink = new GenericContainer<>("eskabetxe/flume")
            .withCopyFileToContainer(MountableFile.forClasspathResource("docker/conf/sink.conf"), "/opt/flume-config/flume.conf")
            .withEnv("FLUME_AGENT_NAME", "docker");

    public GenericContainer<?> source = new GenericContainer<>("eskabetxe/flume")
            .withCopyFileToContainer(MountableFile.forClasspathResource("docker/conf/source.conf"), "/opt/flume-config/flume.conf")
            .withExposedPorts(EXPOSED_PORT)
            .withEnv("FLUME_AGENT_NAME", "docker")
            .dependsOn(sink);

    @BeforeEach
    void start() {
        sink.start();
        source.start();
    }

    @AfterEach
    void stop() {
        source.stop();
        sink.stop();
    }

    protected String getHost() {
        return source.getHost();
    }

    protected Integer getPort() {
        return source.getMappedPort(EXPOSED_PORT);
    }

}