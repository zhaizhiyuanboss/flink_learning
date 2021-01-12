package source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/6 20:32
 */
public class MysqlSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<UserInfo> source = env.addSource(new CustomMysqlSource());

        source.print();
        env.execute("CustomerMySQLSourceDemo");

    }

    public static class CustomMysqlSource extends RichSourceFunction<UserInfo>{

        private Connection connection = null;
        private PreparedStatement ps = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://hadoop01:3306/flinkdemo?useUnicode=true&characterEncoding=utf-8&useSSL=false";
            this.connection = DriverManager.getConnection(url,"root", "root");
            this.ps = connection.prepareStatement("SELECT id,username,password,name FROM user");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (this.ps != null){
                this.ps.close();
            }
            if (this.connection != null) {
                this.connection.close();
            }
        }

        @Override
        public void run(SourceContext<UserInfo> ctx) throws Exception {
            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()){
                int id = resultSet.getInt("id");
                String username = resultSet.getString("username");
                String password = resultSet.getString("password");
                String name = resultSet.getString("name");

                ctx.collect(new UserInfo(id, username, password, name));
            }
        }

        @Override
        public void cancel() {

        }
    }
    /**
     * 构建mysql结果的javabean(POJO)类
     */
    public static class UserInfo {
        private int id;
        private String username;
        private String password;
        private String name;

        @Override
        public String toString() {
            return "UserInfo{" +
                    "id=" + id +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public UserInfo() {
        }

        public UserInfo(int id, String username, String password, String name) {
            this.id = id;
            this.username = username;
            this.password = password;
            this.name = name;
        }
    }
}
