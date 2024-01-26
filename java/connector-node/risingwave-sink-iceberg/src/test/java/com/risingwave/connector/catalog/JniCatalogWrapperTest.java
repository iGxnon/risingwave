package com.risingwave.connector.catalog;

import org.junit.Test;

public class JniCatalogWrapperTest {
    @Test
    public void testJdbc() throws Exception {
        JniCatalogWrapper catalog =
                JniCatalogWrapper.create(
                        "risingwave",
                        "org.apache.iceberg.jdbc.JdbcCatalog",
                        new String[] {
                            "uri", "jdbc:postgresql://192.168.148.3:5432/iceberg",
                            "jdbc.user", "admin",
                            "jdbc.password", "123456",
                            "warehouse", "s3://icebergdata/demo",
                            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                            "s3.endpoint", "http://192.168.148.4:9301",
                            "s3.path-style-access", "true",
                            "s3.access-key-id", "hummockadmin",
                            "s3.secret-access-key", "hummockadmin",
                        });

        catalog.loadTable("s1.t1");
    }
}
