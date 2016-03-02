# Benchmarking project for various serializers of the HDFS ecosystem

## Introduction

For now this is mainly focusing on CSV vs Avro vs Parquet.

Implemented benchmarks:
 - bulk write
 - bulk read/count
 - filtered read, all columns
 - filtered read, some columns
 
The seed (sample) data used for benchmarking is a subset of the KDD data set, containing 49402 lines.


## Results

### Local

#### Configuration
The machine used for the tests is a Mac Notebook Pro with and SSD drive.

#### Results

### Yarn

#### Configuration
The Yarn cluster configuration used consisted of 3 executors with 3 GB RAM each, and a driver having 5 GB of RAM, on top of a 3 nodes HDFS cluster.

#### Results


