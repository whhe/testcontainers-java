package org.testcontainers.oceanbase;

import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers implementation for OceanBase Community Edition.
 * <p>
 * Supported image: {@code oceanbase/oceanbase-ce}
 * <p>
 * Exposed ports:
 * <ul>
 *     <li>SQL: 2881</li>
 *     <li>RPC: 2882</li>
 * </ul>
 */
public class OceanBaseCEContainer extends JdbcDatabaseContainer<OceanBaseCEContainer> {

    static final String NAME = "oceanbasece";

    static final String DOCKER_IMAGE_NAME = "oceanbase/oceanbase-ce";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(DOCKER_IMAGE_NAME);

    private static final Integer SQL_PORT = 2881;

    private static final Integer RPC_PORT = 2882;

    private static final String SYSTEM_TENANT_NAME = "sys";

    private static final String DEFAULT_TEST_TENANT_NAME = "test";

    private static final String DEFAULT_USERNAME = "root";

    private static final String DEFAULT_PASSWORD = "";

    private static final String DEFAULT_DATABASE_NAME = "test";

    private String tenantName = DEFAULT_TEST_TENANT_NAME;

    public OceanBaseCEContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseCEContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        addExposedPorts(SQL_PORT, RPC_PORT);
    }

    @Override
    public String getDriverClassName() {
        return OceanBaseJdbcUtils.getDriverClass();
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl(DEFAULT_DATABASE_NAME);
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        String prefix = OceanBaseJdbcUtils.isMySQLDriver(getDriverClassName()) ? "jdbc:mysql://" : "jdbc:oceanbase://";
        return prefix + getHost() + ":" + getMappedPort(SQL_PORT) + "/" + databaseName + additionalUrlParams;
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
     * Set the non-system tenant to be created for testing.
     *
     * @param tenantName the name of tenant to be created
     * @return this
     */
    public OceanBaseCEContainer withTenant(String tenantName) {
        if (StringUtils.isEmpty(tenantName)) {
            throw new IllegalArgumentException("Tenant name cannot be null or empty");
        }
        if (SYSTEM_TENANT_NAME.equals(tenantName)) {
            throw new IllegalArgumentException("Tenant name cannot be " + SYSTEM_TENANT_NAME);
        }
        this.tenantName = tenantName;
        return self();
    }

    @Override
    protected void configure() {
        if (!DEFAULT_TEST_TENANT_NAME.equals(tenantName)) {
            withEnv("OB_TENANT_NAME", tenantName);
        }
    }
}