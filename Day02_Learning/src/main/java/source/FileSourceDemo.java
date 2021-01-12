package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 18:26
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = env.readTextFile("data/score.csv");
        DataStreamSource<String> source1 = env.readTextFile("hdfs://hadoop01:8020/wordcount/input/wordcount.txt");


        //source.print();
        source1.print();
        env.execute();
    }
}
