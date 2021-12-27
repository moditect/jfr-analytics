# JFR Analytics

An exploration for running analytics on JDK Flight Recorder recordings.

There's two areas of interest:

* Pull-based SQL queries on JFR recording files, using [Apache Calcite](https://calcite.apache.org/)
* Streaming queries on realtime JFR event streams (implementation tbd., e.g. via Apache Flink or Akka Streams)

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

## Running Queries using SQLLine

Using [SQLLine](https://julianhyde.github.io/sqlline/manual.html), you can run ad-hoc SQL queries against a given JFR file.
First build the project using the `sqlline` profile, which will copy SQLLine and all the project dependencies into the _target/lib_ folder.
Then run SQLLine as shown below:

```bash
mvn clean verify -Psqlline -Dquick
java --class-path "target/lib/*:target/jfr-analytics-1.0.0-SNAPSHOT.jar" sqlline.SqlLine
```

Within SQLLine, you can "connect" to a given JFR recording file like so:

```bash
!connect jdbc:calcite:schemaFactory=org.moditect.jfranalytics.JfrSchemaFactory;schema.file=src/test/resources/class-loading.jfr dummy dummy

!tables # shows all tables (i.e. JFR event types)
!columns # shows all columns (i.e. JFR event attributes)
```

## License

This code base is available under the Apache License, version 2.
