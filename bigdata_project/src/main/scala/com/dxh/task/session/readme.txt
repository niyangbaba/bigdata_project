
批处理 事物 手动提交  回滚


sqlParams.indices   ==  0 until sqlParams.length


从HBase中读取数据
val scan = new Scan()
scan.setStartRow(startTime.getBytes())
scan.setStopRow(endTime.getBytes())


val jobConf = new JobConf(configuration)
org.apache.hadoop.hbase.mapreduce.TableInputFormat
jobConf.set(TableInputFormat.INPUT_TABLE, 表名)
jobConf.set(TableInputFormat.SCAN, base64StringScan)


sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
=> tuple2  rowkey,value