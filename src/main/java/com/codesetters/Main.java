package com.codesetters;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Statrted application");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://192.168.29.203:7077");
        sparkConf.setAppName("com.codesetters.Main");
        sparkConf.set("spark.cassandra.connection.host", "192.168.29.203");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        CassandraTableScanJavaRDD<CassandraRow> inputTable = javaFunctions(jsc).cassandraTable("ritam", "post");
        List<CassandraRow> rows = inputTable.collect();
        for (CassandraRow cassandraRow: rows) {
            System.out.println("Cassandra Row "+ cassandraRow);
        }
    }
}
