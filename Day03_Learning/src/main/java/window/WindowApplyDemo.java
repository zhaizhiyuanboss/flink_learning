package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 *  演示窗口的自定义聚合逻辑 apply方法
 *  通过socket获取数据，数据是一个Student类的数据
 *  在window中聚合年龄大于10岁的人的年龄总和
 * @date 2020/9/7 21:35
 */
public class WindowApplyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.socketTextStream("hadoop01", 9999);

        //String -->  Student
        SingleOutputStreamOperator<Student> map = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] strings = value.split(" ");
                return new Student(strings[0], Integer.parseInt(strings[1]));
            }
        });

        //10条
        AllWindowedStream<Student, GlobalWindow> windowedStream = map.countWindowAll(10);


        // 5. 实现apply方法来完成聚合
        SingleOutputStreamOperator<Integer> result = windowedStream.apply(
                new AllWindowFunction<Student, Integer, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<Student> values, Collector<Integer> out) throws Exception {
                int sumAge = 0;
                for (Student value : values) {
                    if (value.age > 10) {
                        sumAge += value.getAge();
                    }
                }
                out.collect(sumAge);
            }
        });

        result.print();
        env.execute();

    }
    public static class Student {
        private String name;
        private int age;

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Student() {}

        public Student(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

}
