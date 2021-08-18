# COVID-19-SPARK 

## A project that utilizes Apache Spark on Scala to perform trend-analysis on the JHU CSSE COVID-19 Dataset.

<p align="center">
 <img width="550" alt="Cover image: 3D heatmap for confirmed COVID-19 cases" src="https://user-images.githubusercontent.com/19466657/129793231-36a3b97a-e97f-4306-8b9f-7270a053ff29.gif">
</p>
<p align="center">
 <i>3D pulse-point map for confirmed COVID-19 cases.<br>Created with flourish.studio.</i>
</p>

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
In your CLI, `git clone https://github.com/kylejwolff/COVID-19-SPARK.git`.

Setting up the environment:
1) Navigate to the root directory of the cloned repository `parent-directory/COVID-19-SPARK/`, rename `build-template.sbt` to `build.sbt`.
2) In the `build.sbt` file, uncomment the appropriate code block to **either** run the application on Spark 2.4.8 or Spark 3.1.2.
3) In your CLI, enter `sbt run` to import dependencies and run the application.

## Usage

On start-up, the application by default loads and cleans all of the dataset CSV files in /raw_data into DataSet objects, so no further pre-processing is required.
Once all pre-processing is completed, the Driver program pauses and waits for user input, as shown below:

<p align="center">
 <img width="700" alt="Driver program menu" src="https://user-images.githubusercontent.com/19466657/129791343-743173d1-35cd-49ba-914f-e3d88d2f298f.png">
</p>

Selecting any of the option will run the corresponding query, where details concerning the query are documented in the source code in Driver.main().

### Visualizations
Although visualization is yet to be implemented as an in-built feature, we have included several visuals produced on [Flourish](https://flourish.studio/) & [Tableau](https://www.tableau.com/) using exported results of queries performed in this application.

<p align="center">
 <img width="700" alt="Query 1: A point-pulse world-map for all confirmed COVID-19 cases" src="https://user-images.githubusercontent.com/19466657/129792133-b38c9cac-b807-4b9e-8e74-1a8885aef67e.gif">
</p>
<p align="center">
 <i>Query 1: A point-pulse world-map for all confirmed COVID-19 cases.</i>
</p>
<br/>
<br/>
<p align="center">
 <img width="700" alt="Query 2: A row-based timeseries for all confirmed COVID-19 cases visualized as a bar-chart race" src="https://user-images.githubusercontent.com/19466657/129792938-5d2d5f6d-6ade-474c-ae95-741b24fd17de.gif">
</p>
<p align="center">
 <i>Query 2: A row-based timeseries for all confirmed COVID-19 cases visualized as a bar-chart race.</i>
</p>

## Contributors

> Kyle Wolff, Brian Jackman, Vincent Chooi.

## License

This project uses the following license: [<license_name>](<link>).
