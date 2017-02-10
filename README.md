# flink-source-code-analysis
*Apache Flink 源码分析系列，基于 git tag 1.1.2*

Apache Flink 被视为第四代的大数据处理框架，它融合了流式计算和批处理【批处理被视为流式计算的特例】

在流式计算方面，使用分布式快照【Checkpoint】实现了高效的数据不丢的机制从而实现 exactly-once 的计算语义；使用 WaterMark 技术实现了窗口计算中延迟数据的处理，同时对流式计算的窗口时间加以分类：processing time、ingestion time、event time

本人觉得 flink 的这些特性一定程序上可以窥探出大数据的未来方向，所以花了些时间来阅读源码，先共享出来希望和大家一起探讨

目前分为一下几个专题：

- flink 基本组件和逻辑计划：介绍了 flink 的基本组件、集群构建的过程、以及客户端逻辑计划的生成过程
- flink 物理计划生成：介绍了 flink JobManager 对逻辑计划的运行时抽象，运行时物理计划的生成和管理等
- jobmanager 基本组件：介绍了 JobManager 的核心组件，它们各自承担的作用
- flink 算子的生命周期：介绍了 flink 的算子从构建、生成、运行、及销毁的过程
- taskmanager 的基本组件：介绍了 flink TaskManager 的核心组件，它们在执行节点上发挥的作用
- flink 网络栈：介绍了 flink 网络层的抽象，包括中间结果抽象、输入输出管理、BackPressure 技术、Netty 连接等
- flink-watermark-checkpoint：介绍 flink 的核心特性：watermark 对计算时间的管理、checkpoint 实现 exactly-once 计算语义
- flink-scheduler：介绍 flink 的任务调度算法及负载均衡
- flink对用户代码异常处理：介绍作业的代码异常后 flink 的处理逻辑，从而更好的理解 flink 是如何保证了 exactly-once 的计算语义



另：本人已将内存更新到博客上： [玉兆的博客](http://chenyuzhao.me) 欢迎访问和吐槽！



本人会陆续更新，欢迎随时交流！

