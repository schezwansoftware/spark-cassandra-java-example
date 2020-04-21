package com.codesetters.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingConfig {

    private JavaStreamingContext jssc;

    public JavaStreamingContext connect() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.setMaster("spark://192.168.29.203:7077");
        sparkConf.setAppName("com.codesetters.spark.Main");
        sparkConf.set("spark.cassandra.connection.host", "192.168.29.203");
        this.jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        return jssc;
    }
}
