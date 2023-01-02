# JFR Analytics

An exploration for running analytics on JDK Flight Recorder recordings.

There's two areas of interest:

* Pull-based SQL queries on JFR recording files, using [Apache Calcite](https://calcite.apache.org/) (work in progress)
* Streaming queries on realtime JFR event streams (implementation tbd., e.g. via Apache Flink or Akka Streams; not started yet)

## Running SQL Queries on JFR Recordings

Each JFR event type gets mapped to a table named after the type, e.g. `jdk.ObjectAllocationSample`, `jdk.ClassLoad`, etc.
Each event attribute gets mapped to a table column.
These tables can be queried programmatically using JDBC or ad-hoc using SQLLine.
See [here](https://bestsolution-at.github.io/jfr-doc/openjdk-17.html) for a list of all built-in JFR event types and their attributes.

### Running Queries Via JDBC

Queries against a JFR file can be run using standard JDBC like shown below
(this query retrieves the ten top allocating stacktraces):

```java
Path jfrFile = ...; // path to some-recording.jfr

Properties properties = new Properties();
properties.put("model", JfrSchemaFactory.INLINE_MODEL.formatted(jfrFile));

try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
    PreparedStatement statement = connection.prepareStatement("""
            SELECT TRUNCATE_STACKTRACE("stackTrace", 40), SUM("weight")
            FROM "JFR"."jdk.ObjectAllocationSample"
            GROUP BY TRUNCATE_STACKTRACE("stackTrace", 40)
            ORDER BY SUM("weight") DESC
            LIMIT 10
            """);

    try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
            System.out.println("Trace : " + rs.getString(1));
            System.out.println("Weight: " + rs.getLong(2));
        }
    }
}
```

### Running Queries Using SQLLine

Using [SQLLine](https://julianhyde.github.io/sqlline/manual.html), you can run ad-hoc SQL queries against a given JFR file.
First build the project using the `sqlline` profile, which will copy SQLLine and all the project dependencies into the _target/lib_ folder.
Then run SQLLine as shown below:

```bash
mvn clean verify -Psqlline -Dquick
java --class-path "target/lib/*:target/jfr-analytics-1.0.0-SNAPSHOT.jar" sqlline.SqlLine
```

Within SQLLine, you can "connect" to a given JFR recording file like so:

```bash
!connect jdbc:calcite:schemaFactory=org.moditect.jfranalytics.JfrSchemaFactory;schema.file=src/test/resources/object-allocations.jfr dummy dummy

!tables # shows all tables (i.e. JFR event types)
!columns "jdk.ObjectAllocationSample" # shows all columns (i.e. JFR event attributes)
!outputformat vertical # vertical output, useful when displaying stack traces

SELECT TRUNCATE_STACKTRACE("stackTrace", 40), SUM("weight")
FROM "jdk.ObjectAllocationSample"
WHERE "startTime" > (SELECT "startTime" FROM "jfrunit.Reset")
GROUP BY TRUNCATE_STACKTRACE("stackTrace", 40)
ORDER BY SUM("weight") DESC
LIMIT 10;
```

### Built-in Functions

There's a set of functions for working with JFR attribute types such as `jdk.jfr.consumer.RecordedClass` and `jdk.jfr.consumer.RecordedStackTrace`.

| Function                                             | Description                                                                                    |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| VARCHAR CLASS_NAME(RecordedClass)                    | Obtains the fully-qualified class name from the given `jdk.jfr.consumer.RecordedClass`         |
| VARCHAR TRUNCATE_STACKTRACE(RecordedStackTrace, INT) | Truncates the stacktrace of the given `jdk.jfr.consumer.RecordedStackTrace` to the given depth |
| BOOL HAS_MATCHING_FRAME(RecordedStackTrace, VARCHAR) | Returns `true` if the given `jdk.jfr.consumer.RecordedStackTrace` contains a frame matching the given regular expression, `false` otherwise |

## Build

Run the following command to build this project:

```bash
mvn clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```bash
mvn clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```bash
mvn process-sources
```

## Using as a library

The easiest way to consume JFR Analytics as a library is to add it as a local dependency, along with Calcite
as a transitive dependency. For example, in this Gradle (Kotlin) snippet:

```kotlin
  implementation(files("lib/jfr-analytics.jar"))
  implementation("org.apache.calcite:calcite-core:1.29.0")
```

## License

This code base is available under the Apache License, version 2.
