# Apache Spark - JDBC Hive Integration
A simple example of [JDBC](http://www.oracle.com/technetwork/java/javase/jdbc/index.html) and [Apache Hive](https://hive.apache.org/) integration in [Apache Spark](https://spark.apache.org/).

## Table of Content
  * [Use Case](#use-case)
  * [Project Description](#project-description)
  * [Prerequisites](#prerequisites)
    + [*nix Systems](#-nix-systems)
    + [Windows Systems](#windows-systems)
  * [How To Run It](#how-to-run-it)
  * [Dataset Description](#dataset-description)
  * [License](#license)

## Use Case
Save relevant information for each delayed flight. A flight is considered delayed if the delay is greater than 15 minutes.

In particular, the following data must be saved:
- tail number (i.e. the civil registration or military serial number)
- aircraft type
- construction year of the aircraft
- flight time (i.e. how long the flight lasted)
- delay
- the ratio of delay to flight time

## Project Description 
In order to get all the required data, two datasets should be used: 
- the Flight dataset
- the Plane dataset

Yet, these two datasets reside on two different systems: 
- the Flight dataset is contained in a structured file loaded into a Hive table
- the Plane dataset is contained in a Relational Database

We need Apache Spark to load both datasets from the respective systems so that the ensuing query can access this data as 
if it were contained in the same system. Once we have the result, we save it in the Relational Database. 

This project doesn't need any Apache Spark, Apache Hive or Relational Database running: everything is executed in memory.

## Prerequisites
This project assumes that both [Java](https://java.com/en/download/) and [SBT](http://www.scala-sbt.org/) are installed.

Moreover, some ulterior assumptions are made based on the system you use. 

### *nix Systems
- You need to have Administrator rights on your machine

### Windows Systems
- You need to have the [winutils.exe binary](https://github.com/steveloughran/winutils/raw/master/hadoop-2.7.1/bin/winutils.exe)
on your machine, and you have to make sure that it is compatible with your system architecture (32- or 64-bit architecture)
- You need to set `HADOOP_HOME` to reflect the directory with `winutils.exe`
- You need to set `PATH` environment variable to include `%HADOOP_HOME%\bin`
- You need to have Administrator rights on your machine. The `run.bat` file must be executed in a command-line window 
(`cmd`) ran as Administrator, i.e. using `Run as administrator` option while executing cmd.

You can find detailed info on how to setup a Windows System [here](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html)

## How To Run It
You need to execute (preferably via CLI) one of the two run scripts included:
- `run.sh` (for *nix systems)
- `run.bat` (for Windows systems)

## Dataset Description
The data consists of flight arrival and departure details for all commercial flights within the USA in 2008.

The Flight dataset is a modified version of the [dataset](http://www.stat.purdue.edu/~lfindsen/stat350/airline2008NovS.txt) 
provided by [Dr. Leonore Findsen](http://www.stat.purdue.edu/~lfindsen/).

The Plane dataset is a modified version of the [dataset](https://github.com/ProjectMOSAIC/databases/blob/master/Data/plane-data.csv) 
provided by [Project](https://github.com/ProjectMOSAIC) [Mosaic](http://mosaic-web.org/).

## License
Unless stated elsewhere, all files herein are licensed under the MIT license. 
For more information, please see the [LICENSE](https://github.com/Vincibean/SparkJdbcHiveIntegration/blob/master/LICENSE) file.