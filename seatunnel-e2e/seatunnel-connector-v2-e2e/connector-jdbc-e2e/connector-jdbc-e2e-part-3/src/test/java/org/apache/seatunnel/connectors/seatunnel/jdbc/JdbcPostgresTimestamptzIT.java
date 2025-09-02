/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcPostgresTimestamptzIT extends TestSuiteBase {

    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private PostgreSQLContainer<?> container;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            c -> {
                Container.ExecResult extra =
                        c.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extra.getExitCode());
            };

    @BeforeAll
    public void startUp() throws Exception {
        container =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("PostgreSQL container started for timestamptz IT");

        Class.forName(container.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initTables);
    }

    @AfterAll
    public void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    @TestTemplate
    public void testTimestamptzRoundtrip(TestContainer testContainer) throws Exception {
        Container.ExecResult result =
                testContainer.executeJob("/jdbc_postgres_timestamptz_roundtrip.conf");
        Assertions.assertEquals(0, result.getExitCode());
        Assertions.assertIterableEquals(
                query("select id, ts_tz from pg_tstz_src order by id"),
                query("select id, ts_tz from pg_tstz_sink order by id"));
    }

    private void initTables() throws SQLException {
        try (Connection conn = getConn();
                Statement st = conn.createStatement()) {
            st.execute(
                    "create table if not exists pg_tstz_src (id int primary key, ts_tz timestamptz)");
            st.execute(
                    "create table if not exists pg_tstz_sink (id int primary key, ts_tz timestamptz)");
            st.execute("truncate table pg_tstz_src");
            st.execute("truncate table pg_tstz_sink");
            // insert a few rows with different offsets and precisions
            st.addBatch(
                    "insert into pg_tstz_src(id, ts_tz) values (1, '2025-05-21T17:57:40+08:00')");
            st.addBatch("insert into pg_tstz_src(id, ts_tz) values (2, '2025-05-21T09:57:40Z')");
            st.addBatch(
                    "insert into pg_tstz_src(id, ts_tz) values (3, '2025-05-21T01:57:40-08:00')");
            st.addBatch(
                    "insert into pg_tstz_src(id, ts_tz) values (4, '2025-05-21T17:57:40.123456+08:00')");
            st.executeBatch();
        }
    }

    private Connection getConn() throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    private List<List<Object>> query(String sql) throws SQLException {
        try (Connection conn = getConn();
                ResultSet rs = conn.createStatement().executeQuery(sql)) {
            List<List<Object>> list = new ArrayList<>();
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                ArrayList<Object> row = new ArrayList<>(n);
                for (int i = 1; i <= n; i++) {
                    Object obj = rs.getObject(i);
                    if (obj instanceof OffsetDateTime) {
                        row.add(obj.toString());
                    } else {
                        row.add(obj);
                    }
                }
                list.add(row);
            }
            return list;
        }
    }
}
