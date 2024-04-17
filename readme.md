# Hadoop Lectures

## Lecture 0 Tools

## Lecture 1 HDFS

## Lecture 2 YARN

[Recommended Video For Introduction YARN](https://www.youtube.com/watch?v=5vmP1-6xd6Y)

Yarn is a programming model/framework for Distributed Computing. It separates resource management and processing components.

Integrates with `HDFS` to provide the exact same benefits for distributed parallel data processing on the cluster.

Uses `Master/Slave` architecture

Yarn framework:

- Schedules and monitors tasks, and re-executes failed tasks
- Sends computations where the data is stored on local disks (data locality).
- Hides complex 'housekeeping' and distributed computing complexity tasks from the developer

**Hadoop Architecture**

Distributed Processing: `Map Reduce`, `Hive`, `Spark`, `Impala`, `Search`

Resource Management: `YARN`, `cgroups`

Distributed Storage: `HDFS`, `NoSQL DataBase`

### YARN Benefits

- Scalability
- Compatibility with MR
- Improved cluster utilization
- Support for workloads other than MR

<img src="Figures\NodeManager.png" alt="NodeManager" style="zoom:50%;" />

A container is a request to hold resources on a `YARN` cluster for an application.

### YARN Daemons

| Component | Description |
| --------- | ----------- |
|           |             |



## Lecture 3 Map Reduce

## Lecture 4 ZooKeeper

## Lecture 5 HA

## Lecture 6 HBase

## Lecture 7 Hive

## Lecture 8 Sqoop

## Lecture 9 Storm

