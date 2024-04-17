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

| Component                | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| Resource Manager (RM)    | Allocates resources<br />RM has two main components: Scheduler & Application Manager |
| Node Manager (NM)        | Each work node has an NM daemon<br />Each NM tracks the available data processing resources and usage |
| Application Manager (AM) | The pre-application AM is responsible for negotiating resources from the RM and working with the NMs to execute and monitor the tasks<br />Runs on a worker node |
| Job History Server       | Achieves jobs and meta-data                                  |

### Schedulers

Some pluggable schedulers are supported in `YARN`:

- FIFO: Allocates resources based on arrival time
- Capacity Scheduler (default): Allocates resources to pools, with FIFO scheduling within each pool
-  Fair Scheduler: Allows `YARN` applications to share resources in large clusters fairly, which is default in `CDH5` and later releases (also used in Oracle DBA)

Configure Fair Scheduler Pluggable Policy:

```xml
# yarn-site.xml
<property>
	<name>yarn.resourcemanager.scheduler.class</name>
	<value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
</property>
```

More details can be found in [hadoop docs](https://hadoop.apache.org/docs/current/hadoop-yarn)

### Commands

`yarn application` commands can be used to list, track, and kill applications:

```
$ yarn application <options>
```

| <options>             | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| -list                 | List applications from the Resource Manager                  |
| -appStates            | Works with `-list` to filter applications based on input comma-separated list of application states such as `ALL`, `NEW`,`NEW_SAVING` |
| -status ApplicationID | Prints the status of the application                         |
| -kill ApplicationID   | Kills the application                                        |

## Lecture 3 Map Reduce

## Lecture 4 ZooKeeper

## Lecture 5 HA

## Lecture 6 HBase

## Lecture 7 Hive

## Lecture 8 Sqoop

## Lecture 9 Storm

