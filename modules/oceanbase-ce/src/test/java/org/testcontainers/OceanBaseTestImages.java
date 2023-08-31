package org.testcontainers;

import org.testcontainers.utility.DockerImageName;

public class OceanBaseTestImages {

    public static final DockerImageName OCEANBASE_CE_IMAGE = DockerImageName.parse("oceanbase/oceanbase-ce:4.1.0.0");
}
