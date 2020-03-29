package com.arvin.flink.demo.dataset.day01;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountExample {

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> text = env.fromElements(
        "Who's there?",
        "I think I hear them. Stand, ho! Who's there?");

    text.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
      String[] words = line.split(" ");
      for (String word : words) {
        out.collect(Tuple2.of(word, 1));
      }
    }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
        .groupBy(0)
        .sum(1)
        .print();
  }
}
