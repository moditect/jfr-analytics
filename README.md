# JFR Analytics

An exploration for running analytics on JDK Flight Recorder recordings.

There's two areas of interest:

* Pull-based SQL queries on JFR recording files, using [Apache Calcite](https://calcite.apache.org/)
* Streaming queries on realtime JFR event streams (implementation tbd., e.g. via Apache Flink or Akka Streams)

## Build

Run the following command to build this project:

```
mvn clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```
mvn clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```
mvn process-sources
```

## License

This code base is available under the Apache License, version 2.
