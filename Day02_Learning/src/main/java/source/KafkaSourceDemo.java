package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 20:21
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        Properties properties = new Properties();
        properties.setProperty("groupid", "itheima");
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        String topic = "kafkatopic";


        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );
        /*
            kafka的source对象可以指定从哪里开始消费
            如:
            kafkaConsumer.setStartFromEarliest();       // 从头开始消费
            kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis()); // 从指定的时间戳开始消费
            kafkaConsumer.setStartFromGroupOffsets();   // 从group 中记录的offset开始消费
            kafkaConsumer.setStartFromLatest();         // 从最新开始消费

            以及指定每个从某个topic的某个分区的某个offset开始消费
            Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new KafkaTopicPartition(topic, 0), 0L);
            offsets.put(new KafkaTopicPartition(topic, 1), 0L);
            offsets.put(new KafkaTopicPartition(topic, 2), 0L);
            kafkaConsumer.setStartFromSpecificOffsets(offsets);
            如上, 就指定了topic的分区0,1,2 都分别从offset 0 开始消费.
         */
        DataStreamSource<String> source = env.addSource(consumer).setParallelism(6);

        source.print();
        env.execute();
    }
}
