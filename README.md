HBase JUnit Rule
=================

JUnit rule to help spin up a HBase mini cluster during unit and integration tests. Works with HBase 1.0.0 and JUnit 4.12+.

Installation
------------

Release are available from Maven Central.

```xml
<dependency>
    <groupId>com.github.charithe</groupId>
    <artifactId>hbase-junit-rule</artifactId>
    <version>1.0.2</version>
</dependency>
```

Usage
-----

```java
@ClassRule
public static HBaseJunitRule hBaseJunitRule = new HBaseJunitRule();

@Test
public void clusterId_notNull() throws IOException {
    Configuration conf = hBaseJunitRule.getHBaseConfiguration();
    try(Connection conn = ConnectionFactory.createConnection(conf)) {
        assertThat(conn.getAdmin().getClusterStatus().getClusterId(), is(notNullValue()));
    }
}
```

In cases where using the JUnit rule is infeasible, HBase mini cluster can be used as follows:

```java
HBaseMiniClusterBooter miniCluster = new HBaseMiniClusterBooter();
try(Connection conn = ConnectionFactory.createConnection(miniCluster.getHBaseConfiguration()){
    ...
}
```
