# opencdc
opensource Change Data Capture
背景介绍：CDC（change data capture）是捕获数据库更改的技术， openCDC是一个开源项目，它提供了一个低延迟的CDC流式处理平台。可以用openCDC监控数据库，应用就可以消费对数据库的每一个行级别(row-level)或者事务级别（transaction level）的更改并为所有的数据库更改事件提供了统一的模型。另外，由于openCDC用持久化、多副本备份的日志来记录数据库数据变化的历史，因此应用可以随时停止再重启，而不会错过它停止运行时发生的事件，保证了所有的事件都能被正确、完整、可靠的处理。

       监控数据库并捕获数据变化一直是很复杂的事情。openCDC比数据库的触发器有非常多的优势：openCDC对所有数据库拥有简单而统一模型，并可与外部进程通信，以及确保以一致的顺序查看和处理所有更改，而且对源数据库性能影响很小。
       
       
使用场景：
  ● 高速缓存更新(Cache invalidation)
       高速缓存中的条目(entry)在源头被更改时立即让缓存中的条目能够及时更新。如果缓存在一个独立的进程中运行(例如Redis，Memcache，Infinispan等)，通过OpenCDC，简单的缓存失效逻辑可以放在独立的进程或服务中，从而简化主应用的逻辑。
       
  ● 简化单体应用(Simplifying monolithic applications)
       许多应用有这样的模式：数据更新提交后，紧接着做一些额外的工作：更新搜索索引，更新缓存，发送通知，运行业务逻辑，等等。这种模式通常称为双写(dual-writes)，而且应用没有在一个事务内写多个系统。这样不仅应用逻辑复杂难以维护，而且容易造成状态不一致。使用openCDC，在源数据库的数据更改提交后，这些额外的工作可以被放在独立的线程或者进程(服务)中完成。这种实现方式的容错性更好，不会丢失事件，容易扩展，并且更容易支持升级。
       
  ● 共享数据库(Sharing databases)
       当多个应用共用同一个数据库的时候，一个应用提交的更改通常要被另一个应用感知到。使用OpenCDC很简单，每个应用可以直接监控和响应数据库的更改。
       
  ● 实时数据集成(Real-time Data integration)
       数据通常被存储在多个地方，尤其是当数据被用于不同的目的的时候，会有不同的形式。保持多系统的同步是很有挑战性的，但是可以通过使用OpenCDC加上简单的事件处理逻辑来实现实时的ETL类型的解决方案。
       
  ● OLTP-OLAP分离
       OLTP-OLAP架构模式中，OLTP使用事务性数据库（Oracle，DB2，SQL server）及相应数据模型，OLAP使用分析型数据库（如列存数据库）及相应的数据模型。OpenCDC可作为OLTP-OLAP之间同步和转换的实时通道，极大提升数据分析的实时性。
