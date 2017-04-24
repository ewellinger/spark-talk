
# Intro

# Intro to Spark

# Spark Vs. Hadoop

## Hadoop Ecosystem
* The Hadoop Ecosystem consists of a variety of modules including:
    * Hadoop Common
    * Hadoop Distributed File System (HDFS)
    * Hadoop YARN
    * Hadoop MapReduce
* When comparing Spark Vs. Hadoop, we are usually comparing Spark Vs. Hadoop MapReduce

## Hadoop MapReduce
* Hadoop MapReduce is a batch-processing engine that exclusively works from disk
    * i.e. MapReduce sequentially reads data from disk, performs an operation on the data, & writing the results back to the cluster
* Specialization of the *split-apply-combine* strategy for data analysis


## Spark
* Comes with a user-friendly API for Scala, Java, Python, R, and Spark SQL
* Capable of running in an interactive mode
* Can work with data in-memory leading to lightning fast data operations
    * Up to 100x faster than Hadoop MapReduce when performing in-memory operations (up to 10x faster on disk)
* Capable of integrating with existing Hadoop ecosystem
    * A Spark application can be run on Hadoop clusters through YARN
    * A Spark application can read directly from HDFS

Spark Streaming (mini-batch processing)


# Spark Execution Model
* Each application in Spark has a driver program that distributes tasks among the executors running on nodes in a cluster
* The *client* (e.g. iPython/iPython Notebook) will have a `SparkContext` which allows you to interact with the driver program on the *master*.  This will...
    * Act as a gateway between the client and the Spark master
    * Sends code/data from iPython to the master (who then sends it to the workers)

Insert the SparkContext -> Driver and Cluster manager picture and maybe move the code example to the next slide

```python
import pyspark as ps

spark = ps.sql.SparkSession.builder \
          .master("local[4]") \
          .appName("Spark Talk") \
          .getOrCreate()
sc = spark.sparkContext
```

## SparkSession in Spark 2.0+

* Prior to Spark 2.0, there were a variety of contexts that were required to interact with different aspects of the Spark ecosystem (e.g. SparkContext, SQLContext, HiveContext, StreamingContext)
* Spark 2.0+ introduces the concept of a SparkSession which can access all of Spark's functionality through a single-unified point of entry
* This serves to minimize the number of concepts to remember or construct as well as making it easier to access DataFrame and Dataset APIs


# RDDs

## Properties of RDDs
* Spark can create RDDs from any storage source supported by Hadoop (e.g. HDFS, HBase, & c.), including local storage
* RDDs can be persistent in order to cache a dataset in memory; this can lead to significant speedups if a dataset needs to be utilized repeatedly in a given application
* RDDs are fault tolerant---if any given partition of an RDD is lost, it will automatically be recomputed by using the recorded transformations in the DAG

Before diving deeper into RDDs, we should touch on what a DAG is...


# Directed Acyclical Graphs
* A Directed Acyclical Graph (or DAG) is how Spark keeps track of what transformations & actions need to be computed
    * Can be thought of how historical events compound upon one another
* An important aspect of Spark is the notion of *Lazy Evaluation*
* All operations fall into one of two categories...
    * **Transformations**: Adds a step to the DAG and returns a new RDD object
    * **Actions**: Forces the execution of the DAG and returns some sort of result


## Notable RDD Transformations
* These will be evaluated *lazily*, i.e. a step will be added to the DAG but **will not** launch any computation

I would throw map, flatMap, filter, sample, distinct, join, reduceByKey, groupByKey here


## Notable RDD Actions
* These will transform an RDD into something else (a python object, or a statistic)
* Actions will launch the processing of the DAG; this is where Spark stops being lazy

collect, count, take, sum, & c.


## Word Count Example

```python
text_rdd = sc.textFile("hdfs://...")
counts = text_rdd.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://...")
```


# Spark SQL & Spark DataFrames

* Unlike the traditional Spark RDD API, the Spark SQL module adds additional information about the schema of the data contained in an RDD, thereby allowing extra optimization
* But what is a schema?
    * Schemas are metadata about your data
    * Schema = Table Names + Column Names + Column Types
* What are the Pros of Schemas?
    * Schemas enable **queries** using SQL and DataFrame syntax
    * Schemas make your data more **structured**


## Specifying Your Own Schema
Let's say we have an RDD with `[(id, name, balance), ... ]` (e.g. `(1234, Erich Wellinger, 42.00)`), we could create a DataFrame like so...

```python
from pyspark.sql.types import *

# Specify schema
schema = StructType( [
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('balance', FloatType(), True),
])

df = spark.createDataFrame(rdd, schema)

# Show schema
df.printSchema()
```

## Inferring a Schema
We can also have Spark infer a schema when reading data in rather than having to specify it manually...

```python
df = spark.read.json('s3n://spark-talk/balance-info.json',
                     inferSchema=True)
```

From here we can use either the DataFrame API or the SQL API to perform operations on our DataFrame object


## Example of DataFrame API
Let's sum the balance for each user-id...

```python
sum_df = df.select(['id', 'balance']) \
           .groupBy('id') \
           .sum('balance')

sum_df.show()
```

## Example of SQL API

```python
df.createOrReplaceTempView('balances')

result = spark.sql('''
    SELECT id, SUM(balance)
    FROM balances
    GROUP BY id
    ''')

result.show()
```


# Spark ML

# LDA

# Demo




### RDDs

* Spark's resilent distributed datasets (RDDs) are how we perform in-memory computations
* An RDD is a collection of immutable partitions of data that are distributed across the nodes of a cluster.


### Transformations And Actions




```bash
aws emr add-steps --cluster-id j-V0PWSYOCK4ZW --steps Type=spark,Name=BookLDAExample,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,spark-talk.py],ActionOnFailure=CONTINUE
```


## Pitfalls of Using Spark
* Under massive development
* Can be a memory hog if jobs are not tuned well, resulting in frustrating out-of-memory errors
* Not all features are available in every API


Apache Hive: Built on top of Hadoop.  Gives a SQL-like interface to query data stored in various databases and file systems

"If Hadoop [HDFS] were ancient Egyptian hieroglyphics, Spark would be the Rosetta stone to understanding the data in Hadoop"
- Donna-M. Fernandez
http://www.metistream.com/comparing-hadoop-mapreduce-spark-flink-storm/



* You should copy the book review dataset to the spark-talk bucket
