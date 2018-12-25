1.将写完的代码打成jar包上传到集群上
2.执行spark——submit脚本 调用sparkSubmit这个类的main方法 通过反射的方式创建spark作业的主类实例对象 调用main方法 执行代码
3.初始化SparkContext对象 在初始化该对象时 会初始化 DAGScheduler和TaskScheduler
4.driver端将请求参数封装进applicationDescription对象，向master进行注册
5.master接受到driver的clientActor的注册请求时，会将请求参数进行封装进APP，加入到master的任务队列中去 对该App持久化
6.轮到我们的任务开始执行时 会调用Scheduler()方法 进行 资源分配 和 任务调度
7.master将分配好的资源 封装到lunchExecutor上 发送到指定的worker上
8.worker接受到master发送过来的lunchExecutor 将其解析并封装到ExecutorRunner中
9.调用Executor的start方法 ，在这个方法中创建了一个线程 在这个线程的run方法中开启了一个用于执行任务的容器 进程
10.Executor开启成功后 向driverActor 反向注册
11.driverActor返回 反向注册成功的消息
12.接受到driverActor发送过来注册成功的消息后 会创建一个线程池 用于接受driverActor发送过来的任务
13.当属于这个任务的所有Executor都反向注册成功后 Driver会结束SparkContext的初始化 继续运行代码
14.driver 端结束SparkContext的初始化后 继续运行代码 创建RDD之间的依赖关系
遇见一个Action算子时 相当于触发了一个 job driver会将这个job提交给DAGScheduler根据RDD之间的依赖关系切分为stage
将stage封装成taskSet 交给 driverActor
15 driverActor 接受到 taskSet 时 会对 每个task进行 序列化 然后将序列化好的task 封装成launchTask 交给executor
16 executor接受到 driverActor 发送过来的launchTask 时 解析并封装到 runnerTask 中
然后从线程池中获取一个线程 进行反序列化  执行编写好的算子 这些算子作用在起保存的 RDD的分区上


























