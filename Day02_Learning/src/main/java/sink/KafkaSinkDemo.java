package sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 21:54
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = env.socketTextStream("hadoop01", 9999);

        // 3. Kafka sink 的构建
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");

        FlinkKafkaProducer<String> kafkatopic = new FlinkKafkaProducer<>(
                "kafkatopic",
                new MyKafkaSerializationSchema(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        source.addSink(kafkatopic);

        env.execute();
    }

    public static class MyKafkaSerializationSchema implements KafkaSerializationSchema<String>{

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<>("kafkatopic", element.getBytes());
    /*
    ProducerRecord有很多构造, 我们使用的是最基础的, 给定topic并且将数据byte化即可,
      即ProducerRecord(String topic, V value)别的构造还会要求传入如：
    - partition号,int类型, 传入啥写哪个分区, 如果不给定的话, 按照key的hash来计算,
      如果没有key的话就按照轮询的方式写入kafka各个分区
    - key, 数据的key, 用以计算key的hash来计算数据落入哪个分区
    - timestamp, 给数据一个指定的时间戳, 如果不设置, 默认以当前系统时间
    上面3个都有默认值, 所以我们不需要设置, 有需要设置可以用其它的重载的构造函数, 如:
    ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)
    */
        }
    }
}
