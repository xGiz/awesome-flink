package com.arvin.flink.demo.dataset.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class DataSetDemo {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSource<Integer> dataSource = env.fromElements(1, 2, 3);
    dataSource.print();

    dataSource.map(i -> i * i).print();

    dataSource
        .flatMap((Integer number, Collector<String> out) -> {
          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < number; i++) {
            builder.append('a');
            out.collect(builder.toString());
          }
        })
        .returns(Types.STRING).print();
  }
}
