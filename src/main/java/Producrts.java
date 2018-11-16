
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * create by newforesee 2018/10/11
 */
public class Producrts {
    public static void main(String[] args) {
        //通过Properties进行配置文件的配置
        Properties prop = new Properties();
        //配置执行的端口号和IP
        prop.put("metadata.broker.list",
                "master:9092,slave1:9092,slave2:9092"
        );
        //进行序列化
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        //自定义一个配置文件
        ProducerConfig config = new ProducerConfig(prop);
        //创建生产者Producrts
        Producer<String,String> producer = new Producer<>(config);
        int i = 0;
        while (true){
            producer.send(new KeyedMessage<String, String>(
                    "test","msg"+i
            ));
            i++;
        }

    }
}
