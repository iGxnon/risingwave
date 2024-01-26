package com.risingwave.connector.catalog;

import org.junit.Test;

public class JniCatalogWrapperTest {
    @Test
    public void testJdbc() throws Exception {
        System.setProperty("aws.region", "us-east-1");
        JniCatalogWrapper catalog =
                JniCatalogWrapper.create(
                        "demo",
                        "org.apache.iceberg.jdbc.JdbcCatalog",
                        new String[] {
                            "uri", "jdbc:postgresql://172.17.0.3:5432/iceberg",
                            "jdbc.user", "admin",
                            "jdbc.password", "123456",
                            "warehouse", "s3://icebergdata/demo",
                            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                            "s3.endpoint", "http://172.17.0.2:9301",
                            "s3.region", "us-east-1",
                            "s3.path-style-access", "true",
                            "s3.access-key-id", "hummockadmin",
                            "s3.secret-access-key", "hummockadmin",
                        });

        System.out.println(catalog.loadTable("s1.t1"));
    }
}
