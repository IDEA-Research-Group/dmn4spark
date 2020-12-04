# dmn4spark

![dmn4spark logo](logo/DMN4Spark.png)

dmn4spark is a library which enables developers to use the Camunda Decision Model and Notation (DMN) in Big Data
environments with Apache Spark.

[Camunda DMN](https://camunda.com/dmn/) is an industry standard for modeling and executing decisions. Decisions are
modeled in DMN tables, a user-friendly way of modeling business rules and decision rules. The rules in the DMN table are
modeled with the Friendly Enough Expression Language ([FEEL](https://docs.camunda.org/manual/7.4/reference/dmn11/feel/)).
[*feel-scala*](https://camunda.github.io/feel-scala/) is the version which is currently supported in this library. It
provides a set of data types and built-in function for making easier the construction of decision rules. Please refer to
its documentation for 

## Versions

* dmn4spark 1.1.1. Dependencies:
    * Scala 2.12
    * Apache Spark 3.0.1
    * [Feel Engine 1.14.0](https://github.com/camunda/feel-scala/tree/1.14.0/feel-engine)
    * [Feel Engine Factory 1.14.0](https://github.com/camunda/feel-scala/tree/1.14.0/feel-engine-factory)
    * [Camunda Engine DMN 7.10.0](https://github.com/camunda/camunda-engine-dmn/tree/7.10.0)


* dmn4spark 1.0.0. Dependencies:
    * Scala 2.12
    * Apache Spark 2.4.0
    * [Feel Engine 1.8.0](https://github.com/camunda/feel-scala/tree/1.8.0/feel-engine)
    * [Feel Engine Factory 1.8.0](https://github.com/camunda/feel-scala/tree/1.8.0/feel-engine-factory)
    * [Camunda Engine DMN 7.10.0](https://github.com/camunda/camunda-engine-dmn/tree/7.10.0)

## Getting started

### Requirements

You need a Maven project with Scala 2.12 and Apache Spark 2.4.0.

### Importing dependencies

Add the following repository to your `pom.xml`

```xml
<repository>
    <id>ext-release-repo</id>
    <name>Artifactory-releases-ext</name>
    <url>http://estigia.lsi.us.es:1681/artifactory/libs-release</url>
    <releases><enabled>true</enabled></releases>
    <snapshots><enabled>false</enabled></snapshots>
</repository> 
```

Add the dmn4spark dependency

```xml
<dependency>
    <groupId>es.us.idea</groupId>
    <artifactId>dmn4spark</artifactId>
    <version>${version.dmn4spark}</version>
</dependency>
```

### Using the library

Then, import the `dmn4spark` implicits:

```scala
import es.us.idea.dmn4spark.implicits._
```

Now, you can load your DMN tables. You just have to call the `dmn` method on any Spark Dataframe. Then, you can specify
the path to your DMN file, which might be located at:

* A local path (use the `loadFromLocalPath` method).
* Hadoop Distributed File System (HDFS) (use the `loadFromHDFS` method).
* A web server (use the `loadFromURL` method).
* Any other source which is not still supported. In this case, you can import the table as an `InputStream` object 
(use the `loadFromInputStream` method).

```scala
df.dmn
  .loadFromLocalPath("models/dmn-file.dmn")
  // or
  .loadFromHDFS("hdfs://my-hdfs/path/to/dmn-file.dmn")
  // or
  .loadFromURL("https://my-webserver.com/path/to/dmn-file.dmn")
  // or
  loadFromInputStream(inputStreamObject)
```

It will execute the Camunda DMN Engine by using the specified DMN table over each row of your Spark Dataframe. The DMN
Engine will take as input each one of the columns of your Dataframe. 

**IMPORTANT**: Make sure that you specify the name of the Dataframe columns (or the name of the output of other DMN 
Tables) as input variables for each DMN Table. 

This operation will create a new Dataframe identical to the original but including the result of each DMN Table. It
means that it will add to your Dataframe as many columns as DMN Tables are in your DMN file. If you don't want to
include all the DMN Tables in the output, you can use the `setDecisions` in order to explicitly select the table outputs
to be included in the resulting Dataframe:

 ```scala
 df.dmn
   .setDecisions("decision1", "decision2")
   .loadFromLocalPath("models/dmn-file.dmn")
 ```

## Future work

* Support other DMN engines.
* Support other languages for the dmn tables (e.g., `juel`)