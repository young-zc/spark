import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * create by newforesee 2018/10/11
 */
public class Consumers {
    public static final String topic = "test";
    public static final Integer threads = 2;
    public static void main(String[] args) {
        //配置相应属性
        Properties properties = new Properties();
        properties.put("zookeeper.connect","master:2181,slave1:2181,slave2:2181");
        //这里可以设置从头开始读取数据
        properties.put("auto.offset.reset","smallest");
        //配置消费者组
        properties.put("group.id","groupA");
        //调用我们配置好的API信息,创建消费者对象
        ConsumerConfig config = new ConsumerConfig(properties);
        //c创建消费者
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);
        //创建map存储多个topic信息
        HashMap<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic,threads);
        //创建获取信息流
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        //循环接收map内的topic数据
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
                        String msg = new String(messageAndMetadata.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }
}
