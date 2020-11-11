# Apache Spark
>  Apache Spark is a unified computing engine and a set of libraries for parallel data processing on computer clusters. Spark is the most actively developed open source engine for this task, making it a standard tool for any developer or data scientist interested in big data.

`spark`起源于`2007`年其创始人`Matei Zaharia`在`UC Bekeley`读博，期间其对数据中心级别的分布式计算非常感兴趣。当时一些互联网公司开始用几千台机器计算并存储数据，`Matei`开始和`Yahoo`以及`Facebook`的团队合作，来解决工业界中的大数据问题。但大多数技术都仅局限于批处理，还缺少对交互式查询的支持并且不支持机器学习等迭代式计算。

另外，`Matei`在`Berkeley`继续研究时发现，`Berkeley`的研究团体同样需要可扩展的数据处理器，特别是机器学习研究。`2009`年`Matei`着手开发`Spark`，并在最初就收到了许多好评，`Berkeley`的许多大数据研究者们都使用`Spark`进行大数据应用开发和研究。其两篇论文很好的阐述了`spark`的设计理念：__Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing__ 和 ___An Architecture for Fast and General Data Processing on Large Clusters___。

### (RDDs) - a distributed memory abstraction

`RDDs`允许应用开发者在大型集群上执行`in-memory`计算，同时保留`MapReduce`等数据流模型的容错能力。`RDDs`的设计受当前数据流系统无法有效处理的两种应用程序的驱动：迭代式计算（`iterative algorithms`）—被广泛用于图计算以及机器学习，以及交互式的数据挖掘工具。为了有效地实现容错，`RDDs`提供了高度受限的共享内存形式：其为只读的数据集（`datasets`）、分区的数据集合（`partitioned collections of records`），并且只能通过其它`RDD`上确定性转换（`map`、`join`和`group by`等）来创建。 `RDD`可通过血统（`lineage`）重新构建丢失的分区数据，其有足够的信息表明如何从其它`RDD`进行转换。

*`Programming Model`*  在`spark`中`rdd`由对象表示，并调用这些对象上的方法进行转换。在定义一个或者多个`RDD`之后，开发者可在操作（`action`）中使用它们，其会将数值返回给应用程序或将数据导出到存储系统。`RDD`仅在`action`中首次使用时才进行计算（they are lazily evaluated），在构建`RDD`时时允许运行时流水线化多个转换。`caching`及`partitioning`是开发者经常控制`RDD`的两个操作，计算后的`RDD`分区数据进行缓存数据之后使用时会加速计算速度。`RDD`通常缓存在内存中，当内存不足时会`spill`到磁盘上。此外，`RDD`通常也允许用户执行分区策略（`partition strategy`），目前支持`hash`和`range`两种方式进行分区。例如，应用程序可对两个`RDD`采用相同的`hash`分区（相同`key`的`record`放在同一台机器），用于加快`join`连接速度。

<img src="example-data/images/spark-narrow-dependency.png" style="zoom:115%;" />

简而言之，每个`RDD`都有一组分区，这些分区都是数据集的原子部分。转换的依赖关系构成了其与父`RDD`的血缘关系：基于其父代计算`RDD`计算的功能及有关其分区方案和数据放置的原数据。`spark`使用`narrow dependencies`和`wide dependencies`来表示`RDD`间的数据依赖，`narrow dependencies`指子`RDD`仅依赖于父`RDD`的固定分区的数据（`each partition of the child RDD depends on a constant number of partitions of parent`），一般为一些`map()`、`filter()`、`union()`、`mapValues()`等转换操作；`wide dependencies`指生成`RDD`的数据依赖与父`RDD`的所有分区数据，常为一些聚合类的转换操作`groupByKey()`、`groupByKey()`、`reduceByKey(func, [numPartitions])`。

`narrow dependencies `允许在集群单台`node`结点上流水线执行父`RDD`所有分区的数据，相比之下，`wide dependencies`则要求所有父`RDD`分区中数据都必须是可用的，以便使用`map-reduce`类似的操作执行跨`node`的`shuffle`操作。除此之外，在`node`计算失败后使用`narrow dependencies`会更加有效，因为其只需计算部分父`RDD`缺失数据的`parition`，并且重新计算的过程可在不同结点上并行进行。而`wide dependencies`则会要求重新计算整个父`RDD`中的所有数据，完整的重新进行计算。

