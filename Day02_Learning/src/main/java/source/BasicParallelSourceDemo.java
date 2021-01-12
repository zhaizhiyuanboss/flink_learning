package source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 18:18
 */
public class BasicParallelSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<Long> source = env.generateSequence(1, 10).setParallelism(6);

        DataStreamSource<Long> source2 = env.fromParallelCollection(
                        new NumberSequenceIterator(1, 10),
                        TypeInformation.of(Long.TYPE)
                ).setParallelism(5);

        source.print();
        //source2.print();

        env.execute();

    }
}
