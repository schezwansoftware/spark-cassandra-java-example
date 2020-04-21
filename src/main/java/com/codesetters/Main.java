package com.codesetters;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Statrted application");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://192.168.29.203:7077");
        sparkConf.setAppName("com.codesetters.Main");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = jsc.textFile("/usr/data/gilbert.txt");
        List<String> collections = inputFile.collect();
        System.out.println("Collections are "+ collections);
    }
}