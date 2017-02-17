#使用Go语言实现简单MapReduce框架

年后回来第一篇！推荐围观简书

[使用Go语言实现简单MapReduce框架](http://www.jianshu.com/p/659d4a396bf7)

学习Go语言也很有一段时间了。这个东西从年前就开始构思，这两天终于研究着搞出来了。算是对于goroutine相关的一个练习吧。

###框架概述

框架的入口为MapReduce容器 MRContainer。使用流程如下：

- 初始化一个MRContainer，指定map与reduce线程数参数。
- 指定map与reduce执行方法。
- 输入数据。
- 调用Start函数启动服务。
- 通过GetResult函数获取结果集。

框架会根据配置开启指定数目的map与reduce协程，然后轮询这些协程以分配任务。最终将结果放在一个list存储的结果集中。