package sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 21:41
 */
public class SinkToLocalFileAndHDFSDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Tuple3<Integer, String, Double>> source = env.fromElements(
                Tuple3.of(19, "潇潇", 170.50),
                Tuple3.of(11, "甜甜", 168.8),
                Tuple3.of(16, "刚刚", 178.8),
                Tuple3.of(19, "蛋蛋", 179.99)

        );

        DataStreamSink<Tuple3<Integer, String, Double>> sink = source.writeAsText(
                "data/output/SinkToLocalFileAndHDFSDemo.txt",
                FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();

    }
}
