package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Lawrence
 * @version 1.0
 * @description: 演示一个基于 处理时间的会话窗口 会话的有效期是 10秒
 * @date 2020/9/7 21:29
 */
public class SessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStreamSource<String> source = env.socketTextStream("hadoop01", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowedStream = map.
                windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        windowedStream.sum(1).print();
        env.execute();

    }
}
