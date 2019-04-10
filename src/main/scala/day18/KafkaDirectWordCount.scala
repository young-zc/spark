package day18

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * creat by newforesee 2018/10/16
  * 直连方式(常用)
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Duration(5000))
    //创建消费者组
    val groupId = "gr"
    //指定topic名字
    val topic = "new"
    //指定Kafka的broker地址(SparkStreaming的Task直接连到Kafka的分区上,用底层API消费,效率更高)
    val brokerList = "master:9092,slave1:9092,slave2:9092"
    //指定zk的地址,后期要更新消费的偏移量是使用(以后可以使用Redis,MySQL来记录偏移量)
    val zk = "master:2181,slave1:2181,slave2:2181"
    //创建topic集合,可以消费多个topic
    val topics = Set(topic)
    //创建一个ZKGroupTopicDirs对象,其实是指定往zk中写入数据的目录用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    //获取zookeeper中的路径 "/gr/offset/new"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    //准备kafka参数
    val kafkas = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    //zookeeper的host和端口创建一个client用于更新偏移量
    //zk客户端,可以从zk读取偏移量,并更新偏移量
    val zkClient = new ZkClient(zk)
    //查询该路径下的分区数
    val clientPartition: Int = zkClient.countChildren(zkTopicPath) //"/gr/offset/new"
    var kafkaStream: InputDStream[(String, String)] = null
    //如果zookeeper中保存有offset,我们会利用这个offset作为kafkaStream的起始位置
    //TopicAndpartition [/gr/offset/new/1/888]
    var formOffset: Map[TopicAndPartition, Long] = Map()
    //如果保存过偏移量
    if (clientPartition > 0) {
      //clientPartition的数量就是/gr/offset/new/下面的分区数目
      for (i <- 0 until clientPartition) {
        val patitionOffset: String = zkClient.readData[String](s"$zkTopicPath/$i")
        val tp = TopicAndPartition(topic, i)
        //将不同的partition对应的offset增加到fromOffset中
        formOffset += (tp -> patitionOffset.toLong)
      }
      //Key:kafka的key  value: "hello hello"
      //这个会将kafka的消息进行transform,最终kafka的数据都会变成(kafka的key,message)
      //这样的Tuple
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd: MessageAndMetadata[String, String]) => (
        mmd.key(), mmd.message()
      )
      //通过kafkaUtils创建直连的DStre(fromOffset参数:按照前面计算好的偏移量继续消费数据)
      kafkaStream = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkas, formOffset, messageHandler)
    } else {
      //如果没有保存,根据Kafka的配置使用最新或者最旧的Offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkas, topics)
    }
    //偏移量范围
    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
    //从kafka读取数据,用Transform方法可以批次提交
    //该Transform方法计算获取到当前批次RDD然后将RDD的偏移量取出来然后再将RDD返回DStream
    //RDD类型是 kafkaRDD

    /*val transform = kafkaStream.transform(rdd => {
      //得到该RDD对应的kafka的消息的Offset
      //该RDD是一个kafkaRDD,可以获得偏移量的范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //取得需要的数据
    val messages: DStream[String] = transform.map(_._2)
*/
    //依次迭代DStream中的RDD
    kafkaStream.foreachRDD((rdd: RDD[(String, String)]) => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //对RDD操作，最后触发Action
      rdd.map((_: (String, String))._2).foreachPartition((f: Iterator[String]) => {
        f.foreach((x: String) => {
          /**
            * 此处
            */
          println(x + "###############")

        })
      })
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        //将该partition的offset保存到zookeeper
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

















