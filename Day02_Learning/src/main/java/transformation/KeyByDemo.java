package transformation;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 20:57
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("篮球", 1),
                Tuple2.of("篮球", 2),
                Tuple2.of("篮球", 3),
                Tuple2.of("足球", 3),
                Tuple2.of("足球", 2),
                Tuple2.of("足球", 3)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.keyBy(0).sum(1);
        sum.print();
        env.execute();
    }
}
