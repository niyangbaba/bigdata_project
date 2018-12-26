bean：和数据库表相关的实体类
caseclass:存放样例类
common:具有总共业务方法的类
conf:存放操作配置文件的类
constants：存放常量
dao:存放操作数据库表相关的类
enum：存放枚举
jdbc:jdbc相关的操作类
kafka：操作kafka相关类
task:spark任务相关类
utils:存放工具类

1. 将 代码 打成jar包 上传集群
2. 执行 sparksubmit 脚本 在 sparksubmit这个类的main方法中 通过反射创建我们spark作业的主类实例对象 执行main方法 运行代码
3. 初始化 sparkcontext对象 同时生成 dagscheluder taskscheduler
4. driver 将请求参数 封装进applicationDescription中 想master 进行任务注册
5. master 接受 到 driver的clientactor 发送过来的注册请求 将请求参数进行解析封装进app 加入到master的任务队列中 进行持久化
6. 轮到我们的任务时 通过scheluder() 进行任务注册 和资源分配
7. master 将分配好的资源 封装进 lunchexecutor中 发送给指定的worker上
8. worker接受到 master发送过来的lunchexecutor是 进行解析并封装到executorrunner中
9. executor的start方法 开启了一个线程 在这个线程的run方法中开启了一个用于接受任务的容器 进程
10.






















