package day18

import java.sql.{Connection, Statement}

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * creat by newforesee 2018/10/16
  */
object foreachRDDAndJDBC {
  //  val updatefunc = (ite: Iterator[(String, Seq[Int], Option[Int])] =>{
  //
  //  })
  val updateFunc: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (ite: Iterator[(String, Seq[Int], Option[Int])]) => {
    ite.map(t => {
      (t._1, t._2.sum + t._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foreachRDDAndJDBC").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://master:9000/checkpoint_foreachRDDAndJDBC")
    //配置kafka需要的参数
    val zk = "master:2181,slave1:2181,slave2:2181"
    val groupId = "gp1"
    val topics: Map[String, Int] = Map[String, Int]("new" -> 1)
    //创建ksfka的输入数据流,获取数据
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zk, groupId, topics)
    //开始处理数据
    val lines: DStream[String] = data.flatMap((_: (String, String))._2.split(" "))
    val words: DStream[(String, Int)] = lines.map((_: String, 1))
    val value: DStream[(String, Int)] = words.updateStateByKey(
      updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      rememberPartitioner = true
    )
    //开始调用foreachRDD
    value.foreachRDD(f => {
      //如果我们要用到Connection连接那么必须用foreachPartition
      f.foreachPartition(fp => {
        //获取JDBC连接
        val conn: Connection = ConnectionPool.getConnection
        fp.foreach(tuple => {
          val sql: String = "insert into streaming(word,count) " +
            "values('" + tuple._1 + "'," + tuple._2 + ")"
          val statement: Statement = conn.createStatement()
          statement.executeUpdate(sql)
        })
        //使用完毕返还连接池
        ConnectionPool.returnConnection(conn)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}











