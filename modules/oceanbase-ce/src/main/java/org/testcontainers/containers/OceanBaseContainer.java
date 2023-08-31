package org.testcontainers.containers;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Testcontainers implementation for OceanBase.
 * <p>
 * Supported image: {@code oceanbase/oceanbase-ce}
 * <p>
 * Exposed ports:
 * <ul>
 *     <li>SQL: 2881</li>
 *     <li>RPC: 2882</li>
 * </ul>
 */
public class OceanBaseContainer extends JdbcDatabaseContainer<OceanBaseContainer> {

    static final String NAME = "oceanbase";

    static final String DOCKER_IMAGE_NAME = "oceanbase/oceanbase-ce";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(DOCKER_IMAGE_NAME);

    private static final Integer SQL_PORT = 2881;

    private static final Integer RPC_PORT = 2882;

    private final String databaseName = "test";
    private final String username = "root@test";
    private final String password = "";

    public OceanBaseContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        this.waitStrategy = Wait.forLogMessage(".*boot success!.*", 1).withStartupTimeout(Duration.ofMinutes(3));

        addExposedPorts(SQL_PORT, RPC_PORT);
    }

    public Integer getSqlPort() {
        return getMappedPort(SQL_PORT);
    }

    public Integer getRpcPort() {
        return getMappedPort(RPC_PORT);
    }

    @Override
    public String getDriverClassName() {
        return "com.oceanbase.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
            "jdbc:oceanbase://" + getHost() + ":" + getMappedPort(SQL_PORT) + "/" + databaseName + additionalUrlParams
        );
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
