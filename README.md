# COVID-19-SPARK 

## A project that utilizes Apache Spark on Scala to perform trend-analysis on the JHU CSSE COVID-19 Dataset.

The main contributions of this project are:

* A Spark environment in which developers can perform high-speed interactive analysis on the JHU CSSE COVID-19 Dataset.
* Tools to load & pre-process the CSV dataset files with the highly-reliable SparkSQL DataSet API.
* Tools to write query results to disk as CSV files for portability.
* Pre-defined queries to extract simple patterns & trends from the original dataset to facilitate more complex queries.
* A Driver program that demonstrates the above functionalities.

## Technologies Used

* Scala - 2.12.10
* sbt - 1.5.4
* Spark-Core - 3.1.2
* Spark-SQL - 3.1.2

To address unresolved build failures on Windows machines using winutils.exe binaries for Hadoop, we have included a compatible technology stack:

* Scala - 2.11.12
* sbt - 1.5.4
* Spark-Core - 2.4.8
* Spark-SQL - 2.4.8

## Features

* tools package - Available tools are for loading, pre-processing & cleaning the original erroneous CSV data & load it as a DataSet object, and write DataSet query results to disk in CSV format.
* queries package - Tools to: 
  * Merge segregated tables (confirmed, recovered & deaths) into unified tables.
  * Extract patterns & trends of interests, such as growth rate of confirmed, recovered and death cases.
  * Convert default column-timeseries to row-timeseries, to increase versatility for use with different visualization tools.
  * Other queries of key-interests, like filter by tropical vs. non-tropical countries, partition by seasons etc.

To-do list:
* Deploy as web application on AWS for public-use.
* Include in-built visualization tools to provide an integrated on-demand analysis & visualization platform, such as Apache Superset.

## Getting Started
   
Cloning the repository:
In your CLI, `git clone https://github.com/kylejwolff/COVID-19-SPARK.git`

Setting up the environment:
In the root directory of the cloned repository `parent-directory/COVID-19-SPARK/`, import dependencies with sbt package & run `sbt run`.

## Usage

> Here, you instruct other people on how to use your project after they’ve installed it. This would also be a good place to include screenshots of your project in action.

## Contributors

> Kyle Wolff, Brian Jackman, Vincent Chooi.

## License

This project uses the following license: [<license_name>](<link>).
