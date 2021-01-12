package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 18:38
 */
public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.socketTextStream("hadoop01", 9999);

        source.print();
        env.execute();
    }
}
