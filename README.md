# parSketch
Massively Distributed Indexing of Time Series for Apache Spark


## Abstract 
This code is a Scala implementation of a sketch/random projection-based method to efficiently perform the parallel indexing of time series and similarity search on them ([RadiusSketch: Massively Distributed Indexing of Time Series.pdf](https://hal-lirmm.ccsd.cnrs.fr/lirmm-01620154/file/ParSketch__DSAA_.pdf)).
 
The method is based on the use of random vectors. The basic idea is to multiply each time series with a set of random vectors. The result of that operation is a ”sketch” for each time series consisting of the distance (or similarity) of the time series to each random vector. Thus two time series can be compared by comparing sketches.

The approach uses a set of grid structures to hold the time series sketches. Each grid contains the sketch values corresponding to a specific set of random vectors over all time series. Two time series are considered to be similar if they are assigned to the same grid cell in a given fraction of grids.

## Architecture

The project is written in Scala on top of Apache Spark. 
ParSketch supports both distributed  and centralized storages for the input data.

ParSketch stores the resulting indexes to a distributed relational storage, setup as a number of PostgreSQL instances. 
Each Spark worker connects to one of the databases through JDBC and persists the assignments of each time series to grid cells as soon as they are identified. 
This pipelined procedure avoids the in-memory storage of large intermediate datasets, hence reduces the memory consumption during grid construction, thus avoiding memory overflow at Spark workers.
Moreover, the embedded features of RDBMS, such as indexing (to boost the handling of predicates that express the search in grids),  grouping and aggregation, are taken advantage of for more efficient query processing.



## Getting Started
 
### Prerequisites

Resource Name | Resource Description | Supported Version  | Remarks
------------ | ------------- | ------------- | -------------
Oracle Java | The Java Runtime Environment (JRE) is a software package to run Java and Scala applications | 8.0
Apache Hadoop | Hadoop Distributed File System (HDFS™): A distributed file system that provides high-throughput access to application data | v. 2.7.x 
Apache Spark | Large-scale data processing framework | v. 2.1.0 or later 
PostgreSQL | Relational database system to store indices and to provide more effective Query Processing on indexed data | v. 9.3.x or later| One instance of PostgreSQL server should be running on each node of a cluster. 


### Installing 

The code is presented as a Maven-built project. An executable jar with all dependencies can be built with the following command:

`mvn clean package
`

## Running

You can run each stage (Grid (Index) Construction and Query Processing) as a separate call with the list of required options:

<pre>
#Grid construction
    $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.TSToDBMulti parSketch-2.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val --gridConstruction true  --numPart int_val --tsNum int_val --sampleSize dec_val
	
#Index creation
   $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.TSToDBMulti parSketch-2.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --batchSize int_val  --numPart int_val --tsNum int_val
    
#Query processing
   $SPARK_HOME/bin/spark-submit --class fr.inria.zenith.TSToDBMulti parSketch-2.0-SNAPSHOT-jar-with-dependencies.jar --tsFilePath path --sizeSketches int_val --gridDimension int_val --gridSize int_val --queryFilePath path --candThresh dec_val --numPart int_val --tsNum int_val --topCand int_val
    
    Options:
    
    --tsFilePath            Path to the Time Series input file
    --sizeSketches          Size of Sketch [Default: 30] 
    --gridDimension         Dimension of Grid cell [Default: 2]
    --gridSize              Size of Grid cell [Default: 2]
    --tsNum                 Number of Time Series
    --batchSize             Size of Insert batch to DB [Default: 1000]
    --gridConstruction      Boolean parameter [Default: false]
    --numPart               Number of partitions for parallel data processing
    
    --queryFilePath         Path to a given collection of queries
    --candThresh            Given threshold (fraction) to find candidate time series from the grids  
    --queryResPath          Path to the result of the query (Optional)
    --saveResult            Boolean parameter, if true - save result of query to file, false - statistics output to console [Default: true]
    --topCand               Number of top candidates (based on Euclidean distance) to save  [Default: 5]
    --nodesFile             Path to the list of cluster nodes (hostname ips) [Default: nodes]
    
    --gridsResPath          Path to the result of grids construction (Optional)
    --jdbcDriver            JDBC driver to RDB  [Default: PostgreSQL] (Optional)
    --jdbcUsername          Username to connect to DB (Optional)
    --jdbcPassword          Password to connect to DB (Optional)
    
    --sampleSize            Size of sample to build progressive grid sizes (Optional) [Default: 0.1]
</pre>


## Datasets 
As an input parSketch accepts data in txt, csv and object files. The tool supports distributed or centralized file systems. 

For experimental verification synthetic and real datasets were used.
 
Random Walk dataset generated by [Random Walk Time Series Generator](https://github.com/lev-a/RandomWalk-tsGenerator).  At each time point the generator draws a random number from a Gaussian distribution N(0,1), then adds the value of the last number to the new number.



## License
Apache License Version 2.0