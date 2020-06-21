This repository contains the Java labs as well as their Scala and Python ports of the code used in Manning Publication’s **[Spark in Action, 2nd edition](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp)**, by Jean-Georges Perrin.

# Spark in Action, 2nd edition – Java, Python, and Scala code for chapter 14

Chapter 14 is about **extending data transformation with UDFs** (user defined functions).

This code is designed to work with Apache Spark v2.4.4.

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**. This chapter has several labs.

### Lab \#200

Using a UDF using the dataframe API.

### Lab \#210

Using a UDF using SparkSQL.

### Lab \#900

A simple UDF to see if a value is in range.

### Lab \#910, \#911, and \#912

Attempts at using polymorphism with UDFs.

### Lab \#920

Passing an entire column to a UDF.

## Datasets

Dataset(s) used in this chapter:
* South Dublin (Republic of Ireland) County Council's [libraries](https://data.smartdublin.ie/dataset/libraries).

The `OpenedLibrariesApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	Spark stores the contents in a dataframe, then demonstrate hhow to use Custom UDF to check if in range.

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch14
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch14/src/main/python/lab200_library_open/
```

3. Execute the following spark-submit command to create a jar file to our this application

 ```
spark-submit openedLibrariesApp.py
 ```

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch14
```

2. cd net.jgp.books.spark.ch14

3. Package application using sbt command

```
sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch14.lab200_library_open.OpenedLibrariesScalaApp target/scala-2.12/SparkInAction2-Chapter14-assembly-1.0.0.jar
```

## Notes
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://facebook.com/sparkinaction/) or in [Manning's live site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
