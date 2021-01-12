package cn.itcast.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.concurrent.ExecutionException;

/**
 * @author Lawrence
 * @version 1.0
 * @description:
 * @date 2020/9/4 21:44
 */
public class TestSource {
    public static void main(String[] args) throws Exception {
        //env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //source:The elements must all be of the same type
        DataSource<String> dataSource = env.fromElements("haha");

        dataSource.print();
    }
}
