package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Lawrence
 * @version 1.0
 * @description:  演示一个 滚动的 时间窗口(处理时间)
 * @date 2020/9/7 20:48
 */
public class TumblingTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //source
        DataStreamSource<String> source = env.socketTextStream("hadoop01", 9999);

        //处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value.split(" ")[0], Integer.parseInt(value.split(" ")[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        //设置窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream
                .timeWindow(Time.seconds(10));

        //将设置好的数据累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);


        result.print();

        env.execute();


    }
}
