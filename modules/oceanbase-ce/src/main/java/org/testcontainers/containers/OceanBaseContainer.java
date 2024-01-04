package org.testcontainers.containers;

import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

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

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;

    private static final Integer SQL_PORT = 2881;
    private static final Integer RPC_PORT = 2882;

    private static final String SYSTEM_TENANT = "sys";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_TEST_TENANT_NAME = "test";
    private static final String DEFAULT_DATABASE_NAME = "test";

    private String mode;
    private String tenantName = DEFAULT_TEST_TENANT_NAME;

    public OceanBaseContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        preconfigure();
    }

    private void preconfigure() {
        this.waitStrategy =
            new LogMessageWaitStrategy()
                .withRegEx(".*boot success!.*")
                .withTimes(1)
                .withStartupTimeout(Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS));

        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
        addExposedPorts(SQL_PORT, RPC_PORT);
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    public Integer getSqlPort() {
        return getActualPort(SQL_PORT);
    }

    public Integer getActualPort(int port) {
        return "host".equals(getNetworkMode()) ? port : getMappedPort(port);
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(DEFAULT_DATABASE_NAME);
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://" + getHost() + ":" + getSqlPort() + "/" + databaseName + additionalUrlParams;
    }

    @Override
    public String getDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    @Override
    public String getUsername() {
        return DEFAULT_USERNAME + "@" + tenantName;
    }

    @Override
    public String getPassword() {
        return DEFAULT_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    /**
     * Set the deployment mode, see <a href="https://hub.docker.com/r/oceanbase/oceanbase-ce">Docker Hub</a> for more details.
     *
     * @param mode the deployment mode
     * @return this
     */
    public OceanBaseContainer withMode(String mode) {
        this.mode = mode;
        return self();
    }

    /**
     * Set the non-system tenant to be created for testing.
     *
     * @param tenantName the name of tenant to be created
     * @return this
     */
    public OceanBaseContainer withTenant(String tenantName) {
        if (StringUtils.isEmpty(tenantName)) {
            throw new IllegalArgumentException("Tenant name cannot be null or empty");
        }
        if (SYSTEM_TENANT.equals(tenantName)) {
            throw new IllegalArgumentException("Tenant name cannot be " + SYSTEM_TENANT);
        }
        this.tenantName = tenantName;
        return self();
    }

    @Override
    protected void configure() {
        if (StringUtils.isNotBlank(mode)) {
            withEnv("MODE", mode);
        }
        if (!DEFAULT_TEST_TENANT_NAME.equals(tenantName)) {
            withEnv("OB_TENANT_NAME", tenantName);
        }
    }
}
