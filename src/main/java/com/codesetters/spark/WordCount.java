package com.codesetters.spark;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class WordCount {

    private final JavaStreamingContext jssc;
    private static final Pattern SPACE = Pattern.compile(" ");


    public WordCount(JavaStreamingContext jssc) {
        this.jssc = jssc;
    }

    public void startCounting() throws InterruptedException {
        System.out.println("Word counting has begun");
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.29.203", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> {
            System.out.println(s);
            return new Tuple2<>(s, 1);
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

// Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
