# Getting Started with MapR Streams

MapR Streams is a core component to the MapR Converged Data Platform. It is a distributed framework for storing and accessing real-time data streams.

This guide will show you how to work with MapR Streams using:

- The Kafka API for sending and receiving primitive messages
- Data conversion techniques for streaming rich data structures such as JSON and POJOs
- The OJAI library for persisting streaming data to MapR-DB
- Apache Spark for consuming and processing streaming data

## Background

When data structures are communicated through MapR Streams they must be converted to Strings or Byte arrays. This is straightforward for data types such as JSON since unicode characters can be easily converted to Strings, but other data structures such as Java Objects require user-defined schemas or data classes in order to reconstruct the original data type from streamed Byte arrays. The following examples are provided in this project to show common patterns for streaming JSON and Java Objects through MapR Streams:

1. Streaming plain text messages
2. Streaming rich data structures with Avro encoding
3. Streaming plain-old-java-objects (POJOs)
4. Streaming JSON documents and persisting each message to MapR-DB tables

## Prerequisites

* MapR Converged Data Platform 6.0 cluster or [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* Maven 3.x

The MapR Container For Developers is a docker image that enables you to quickly deploy a single-node MapR instance on a workstation. It is designed to give software developers an easy way to experiment with the APIs for MapR Streams, MapR-DB, and Spark without the burden of connecting to a real cluster. Installation steps can be found [**here**](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html). The installation basically consists of running the following commands on your Mac:

```
$ wget http://package.mapr.com/releases/v6.0.0/mac/mapr-client-6.0.0.20171109191718.GA-1.x86_64.tar.gz -P ~/Downloads/
$ sudo mv ~/Downloads/mapr-client-6.0.0.20171109191718.GA-1.x86_64.tar.gz /opt
$ cd /opt
$ sudo tar xvfz mapr-client-6.0.0.20171107171328.GA-1.x86_64.tar.gz
$ cd ~/
$ wget https://raw.githubusercontent.com/mapr-demos/mapr-db-60-getting-started/master/mapr_devsandbox_container_setup.sh
$ chmod +x mapr_devsandbox_container_setup.sh
$ ./mapr_devsandbox_container_setup.sh
```

The rest of this guide will assume you're running the MapR Container For Developers in Docker on a Mac and have installed MapR client software on that same Mac.

## Step 1: Verify the MapR cluster is working

It can take up to 5 minutes for the cluster to start. You'll know it's finished starting if you can connect to the container with `ssh root@localhost -p 2222` using password "mapr" and the /apps/ cluster directory is shown by the command, `/opt/mapr/bin/hadoop fs -ls /apps`. If the container is taking longer than a 5 minutes to start, then you probably need to allocate more memory to Docker (try 8GB) or otherwise decrease memory pressure by closing some apps.

## Step 2: Create a stream and topic

Define a stream and topic using the following command on your MapR node:

```
$ maprcli stream create -path /apps/mystream -produceperm p -consumeperm p -topicperm p -ttl 900
$ maprcli stream topic create -path /apps/mystream -topic mytopic -partitions 3
```

List the topic like this to make sure it was created:

```
$ maprcli stream topic list -path /apps/mystream
topic            partitions  logicalsize  consumers  maxlag  physicalsize
trades           1           0            0          0       0
```

## Step 3: Build this project and copy the jar file to a MapR cluster node

Build the project with this command:
```
mvn package
```

Get the container ID for the maprtech/dev-sandbox-container:latest with the following command:
```
docker ps
```

The container ID will look something like "cef0f5194658". Use that container ID to copy this project's jar file to the MapR node:
```
docker cp ./target/mapr-streams-study-1.0-jar-with-dependencies.jar CONTAINER_ID:/root/
```

## Step 4: Stream plain text messages

Connect to the MapR node (`ssh root@localhost -p 2222`) and run the consumer like this:

```
java -cp target/mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.BasicConsumer /apps/mystream:mytopic
```

Run a producer in another ssh connection, like this:

```
java -cp .:./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.BasicProducer /apps/mystream:mytopic
```

Now, type some stuff in the producer and you should see it received on the consumer.

You just streamed plain text messages through MapR Streams. However, applications often need to stream data, not just plain-text messages. The next example shows how to do that.

## Step 5. Stream JSON data with Avro encoding

[Avro](https://avro.apache.org/docs/current/) is a data encoding library that uses user-defined schemas to convert rich data structures to compact Byte arrays for streaming. You can stream Avro encoded messages on your MapR node with the following two commands:

```
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.AvroConsumer /apps/mystream:mytopic2
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.AvroProducer /apps/mystream:mytopic2
```

The producer will stream a couple hundred Avro encoded messages and the consumer will decode them and print their contents. Note how we specified a different topic than in the last example. Here we're using "mytopic2". We can't use the topic in the past example (unless we delete it) because it contains messages that don't comply with the schema we defined in Avro. If we did try to attach avroconsumer to "mystream:mytopic", it would fail. Avro was designed to provide this kind of schema enforcement for data validation purposes.

## Step 6. Stream plain-old-java-objects (POJOs)

This example shows how to convert POJOs to binary streams and back. It also shows how to invoke a synchronous callback after a data record has been sent by a stream producer. Run these examples with the following commands:

```
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar comapr.examples.PojoConsumer /apps/mystream:mytopic3
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.PojoProducer /apps/mystream:mytopic3
```

The two examples we just discussed for streaming Avro encoded data and POJOs are tied to a specific schema. If you accidentally publish a different type of message to the stream the consumers will fail. That kind of schema enforcement is sometimes desirable for data validation, but contrast that with the next example which encodes data as JSON messages and consequently provides the flexibility for a single stream to be used for schema-free data.

## Step 7. Stream JSON data and persisting each message to MapR-DB tables

This example shows how to stream JSON data and persist each message to MapR-DB. Akka is used to asynchronously parse and save the streamed JSON messages to MapR-DB. This way we can avoid blocking stream reads when we're parsing and persisting messages, which is important since we can read from a stream faster than we can persist to disk. Unlike to previous two examples, we're encoding the streamed JSON data as Strings (not Byte arrays). The Akka message processor converts each message to a JSON document using the Open JSON Application Interface (OJAI) and persists that to MapR-DB JSON tables.

This example is pretty cool, because it shows how consumers can process streaming messages asynchronously using Akka.  It's also cool because it shows how you can stream rich data structures (i.e. JSON) and process them without schema restrictions. Run the AkkaProducer with any JSON file, and the consumer will save those records to a MapR-DB JSON table. Here's how to run the producer and consumer:

```
wget https://raw.githubusercontent.com/mapr-demos/customer360/master/clickstream/data/clickstream_data.json
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.AkkaProducer /apps/mystream:mytopic4 clickstream_data.json
java -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.AkkaConsumer /apps/mystream:mytopic4 /apps/mytable
```

After the consumer starts persisting messages, you can use Drill to inspect what was inserted into MapR-DB with the following command (run this on the MapR node):

```
$ /opt/mapr/drill/drill-1.11.0/bin/sqlline -u "jdbc:drill:drillbit=localhost" -n mapr
0: jdbc:drill:drillbit=localhost> select count(*) from `dfs.default`.`./apps/mytable`;
```

You can also inspect the table with DB shell from the MapR node:

```
$ /opt/mapr/bin/mapr dbshell
maprdb root:> jsonoptions --pretty true --withtags false
maprdb root:> find /apps/mytable --limit 2
```

For more information about using MapR-DB in the MapR developer sandbox, check out the excellent tutorial at [https://github.com/mapr-demos/mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started).

## Step 8. Stream JSON data and consume with Spark Streaming

Here's how to run Run this on your Mac:
```
java -cp target/mapr-streams-study-1.0-jar-th-dependencies.jar com.mapr.examples.ClickstreamConsumer /apps/mystream:mytopic
```

## How to debug with breakpoints in IntelliJ

IntelliJ and many other Java IDEs provide elegant debuggers with which you can set breakpoints and inspect programs executing on remote hosts. Here's how to attach the IntelliJ debugger to any Java process you start in the MapR developer container:

Open this project in IntelliJ and create a Remote run configuration like this:

1. Run -> Edit Configurations...
2. Click the "+" in the upper left
3. Select the "Remote" option in the left-most pane
4. Choose a name (I named mine "remote-debugging")
5. Change the debugger port to 4040 (because this is one of the open ports in the MapR developer container)
6. Click "OK" to save.

![intellij_debug_config.png](https://github.com/iandow/mapr-streams-study/blob/master/images/intellij_debug_config.png)

Now, insert "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4040" in the Java command you wish to debug. For example, to debug the POJO Consumer shown above you would run:

```
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4040 -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar comapr.examples.PojoConsumer /apps/mystream:mytopic3
```

Then run the "remote-debugging" configuration you setup in IntelliJ. IntelliJ will connect to the JVM and initiate remote debugging.

## Cleaning Up

When you are done, you can delete the stream, and all associated topic using the following command:
```
$ maprcli stream delete -path /apps/mystream
$ hadoop fs -rm /apps/mytable
```
