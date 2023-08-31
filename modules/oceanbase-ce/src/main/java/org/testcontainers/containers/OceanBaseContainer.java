package org.testcontainers.containers;

import org.apache.commons.lang3.StringUtils;
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

    private static final String SYSTEM_TENANT = "sys";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_TENANT_NAME = "test";
    private static final String DEFAULT_DATABASE_NAME = "test";

    /**
     * The deployment mode of OceanBase. See <a href="https://hub.docker.com/r/oceanbase/oceanbase-ce">Docker Hub</a> for more details.
     */
    enum Mode {
        /**
         * Standard standalone deployment with a monitor service.
         */
        NORMAL,

        /**
         * Similar to 'normal' mode, but uses less hardware resources.
         */
        MINI,

        /**
         * Standalone deployment without the monitor service, which uses the least hardware resources.
         */
        SLIM;

        static Mode fromString(String mode) {
            if (StringUtils.isEmpty(mode)) {
                throw new IllegalArgumentException("Mode cannot be null or empty");
            }
            switch (mode.trim().toLowerCase()) {
                case "normal":
                    return NORMAL;
                case "mini":
                    return MINI;
                case "slim":
                    return SLIM;
                default:
                    throw new IllegalArgumentException("Unsupported mode: " + mode);
            }
        }
    }

    private Mode mode = Mode.SLIM;
    private String sysRootPassword = DEFAULT_PASSWORD;
    private String tenantName = DEFAULT_TENANT_NAME;

    public OceanBaseContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        this.waitStrategy = Wait.forLogMessage(".*boot success!.*", 1).withStartupTimeout(Duration.ofMinutes(3));

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
     * Set the deployment mode.
     *
     * @param mode the deployment mode, can be 'slim', 'mini' or 'normal'
     * @return this
     */
    public OceanBaseContainer withMode(String mode) {
        this.mode = Mode.fromString(mode);
        return self();
    }

    /**
     * Set the root password of sys tenant.
     *
     * @param sysRootPassword the root password of sys tenant
     * @return this
     */
    public OceanBaseContainer withSysRootPassword(String sysRootPassword) {
        if (sysRootPassword == null) {
            throw new IllegalArgumentException("The root password of sys tenant cannot be null");
        }
        this.sysRootPassword = sysRootPassword;
        return self();
    }

    /**
     * Set the non-system tenant name to be created for testing.
     *
     * @param tenantName the tenant name to be created
     * @return this
     */
    public OceanBaseContainer withTenantName(String tenantName) {
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
        withEnv("MODE", mode.toString().toLowerCase());
        withEnv("OB_ROOT_PASSWORD", sysRootPassword);
        withEnv("OB_TENANT_NAME", tenantName);
    }
}
