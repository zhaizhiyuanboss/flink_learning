package source;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @author Lawrence
 * @version 1.0
 * @description:自定义source
 * @date 2020/9/6 19:41
 */
public class CustomerSourceWithoutParallelDemo {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<Order> source = env.addSource(new MySource());

        source.print();
        env.execute();
    }
    public static class MySource implements SourceFunction<Order> {
        private boolean isRun = true;
        @Override
        public void run(SourceContext ctx) throws Exception {
            Random random = new Random();
            while (isRun){
                String id = UUID.randomUUID().toString();
                String userId = random.nextInt(99) + "";
                int money = random.nextInt(999);
                long time = System.currentTimeMillis();


                ctx.collect(new Order(id,userId,money,time));
            }
        }

        @Override
        public void cancel() {
            this.isRun = false;
        }
    }

    public static class Order {
            private String id;
            private String userId;
            private int money;
            private long time;

            public Order(String id, String userId, int money, long time) {
                this.id = id;
                this.userId = userId;
                this.money = money;
                this.time = time;
            }
            public Order() {}

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            public String getUserId() {
                return userId;
            }

            public void setUserId(String userId) {
                this.userId = userId;
            }

            public int getMoney() {
                return money;
            }

            public void setMoney(int money) {
                this.money = money;
            }

            public long getTime() {
                return time;
            }

            public void setTime(long time) {
                this.time = time;
            }

            @Override
            public String toString() {
                return "Order{" +
                        "id='" + id + '\'' +
                        ", userId='" + userId + '\'' +
                        ", money=" + money +
                        ", time=" + time +
                        '}';
            }
        }

}
