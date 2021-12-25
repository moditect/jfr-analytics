/*
 *  Copyright 2021 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.moditect.jfranalytics;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JfrSchemaFactoryTest {

    @Test
    public void canRetrieveTables() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", getConnectionProperties("basic.jfr"))) {
            DatabaseMetaData md = connection.getMetaData();
            try (ResultSet rs = md.getTables(null, "jfr", "%", null)) {
                Set<String> tableNames = new HashSet<>();

                while (rs.next()) {
                    tableNames.add(rs.getString(3));
                }

                assertThat(tableNames).containsExactlyInAnyOrder("jdk.GarbageCollection", "jdk.ThreadSleep", "jfrunit.Sync");
            }

            try (ResultSet rs = md.getColumns(null, "jfr", "jdk.ThreadSleep", null)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("startTime").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("TIMESTAMP(0)").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("duration").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("eventThread").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("VARCHAR").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("stackTrace").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("VARCHAR").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("time").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isFalse();
            }

            try (ResultSet rs = md.getColumns(null, "jfr", "jdk.GarbageCollection", null)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("startTime").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("TIMESTAMP(0)").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("duration").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("gcId").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("name").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("VARCHAR").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("cause").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("VARCHAR").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("sumOfPauses").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(4)).isEqualTo("longestPause").describedAs("column name");
                assertThat(rs.getString(6)).isEqualTo("BIGINT").describedAs("type name");

                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void canRunSimpleSelectFromThreadSleep() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", getConnectionProperties("basic.jfr"))) {
            PreparedStatement statement = connection.prepareStatement("""
                    SELECT "startTime", "time", "eventThread", "stackTrace"
                    FROM "jfr"."jdk.ThreadSleep"
                    WHERE "time" = 1000000000
                    """);

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getTimestamp(1)).isEqualTo(Timestamp.from(ZonedDateTime.parse("2021-12-23T13:40:50.402000000Z").toInstant()));
                assertThat(rs.getLong(2)).isEqualTo(1_000_000_000L);
                assertThat(rs.getString(3)).isEqualTo("main");
                assertThat(rs.getString(4)).startsWith("""
                        {
                          truncated = true
                          frames = [
                            java.lang.Thread.sleep(long)
                             org.moditect.jfrunit.demos.todo.HelloJfrUnitTest.basicTest() line: 24
                        """);
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void canRunSimpleSelectFromGarbageCollection() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", getConnectionProperties("basic.jfr"))) {
            PreparedStatement statement = connection.prepareStatement("""
                    SELECT "startTime", "duration", "gcId", "name", "cause", "sumOfPauses", "longestPause"
                    FROM "jfr"."jdk.GarbageCollection"
                    """);

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();

                assertThat(rs.getTimestamp(1)).isEqualTo(Timestamp.from(ZonedDateTime.parse("2021-12-23T13:40:50.384000000Z").toInstant()));
                assertThat(rs.getLong(2)).isEqualTo(17717731L);
                assertThat(rs.getInt(3)).isEqualTo(2);
                assertThat(rs.getString(4)).isEqualTo("G1Full");
                assertThat(rs.getString(5)).isEqualTo("System.gc()");
                assertThat(rs.getLong(6)).isEqualTo(17717730L);
                assertThat(rs.getLong(7)).isEqualTo(17717730L);
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    public void canRunAggregation() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", getConnectionProperties("basic.jfr"))) {
            PreparedStatement statement = connection.prepareStatement("""
                    SELECT count(*), sum("time")
                    FROM "jfr"."jdk.ThreadSleep"
                    """);

            try (ResultSet rs = statement.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(51);
                assertThat(rs.getLong(2)).isEqualTo(5_850_000_000L);
                assertThat(rs.next()).isFalse();
            }
        }
    }

    private Properties getConnectionProperties(String jfrFileName) {
        Path jfrFile = getTestResource(jfrFileName);
        Properties properties = new Properties();
        properties.put("model", JfrSchemaFactory.getInlineModel(jfrFile));

        return properties;
    }

    private Path getTestResource(String resource) {
        try {
            Path path = Path.of(JfrSchemaFactoryTest.class.getResource("/" + resource).toURI());

            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Couldn't find resource: " + path);
            }

            return path;
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
