
spark 任务 将日志处理存储到HBase中
1. 参数的判断与验证

simpleDateFormat parse format


匹配器    编译器       去匹配             是否匹配到   返回 true/false
Pattern.compile(reg).matcher(inputDate).matches()

FileSystem 对象 hdfs上的文件系统

* collect算子:
* 1，触发任务提交
* 2，将结果从executor端拉回到driver端


rowKey  唯一，散列，长度不宜过长，方便查询

val put = new Put(rowKey.getBytes())
put.addColumn(列族.getBytes(), 列.getBytes(), 值.getBytes())

(new ImmutableBytesWritable(), put)
TableOutputFormat extends FileOutputFormat<ImmutableBytesWritable, Put>

jobConf.setOutputFormat(classOf[TableOutputFormat])
jobConf.set(TableOutputFormat.OUTPUT_TABLE, 表名)
tuple2Rdd.saveAsHadoopDataset(jobConf)


ip2Long  将ip转换为long类型 192.168.233.101 -> 1987987948

二分法











