## COVID-19-SPARK 

### create build.sbt in project folder
for spark 3:
````
name := "COVID-19-SPARK"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.1.2",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)
````

