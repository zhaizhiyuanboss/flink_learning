package source;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 18:14
 */
public class BasicSourceDemo {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source1 = env.fromElements("hadoop", "hive");
        DataStreamSource<String> source2 = env.fromCollection(Arrays.asList("hadoop", "hive"));

        source1.print();
        source2.print();

        env.execute();
    }
}
