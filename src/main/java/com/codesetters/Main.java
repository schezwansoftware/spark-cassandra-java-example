package com.codesetters;

import com.codesetters.domain.Post;
import com.codesetters.domain.PublisherPostStats;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.datastax.spark.connector.rdd.CassandraTableScanRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;
import scala.Tuple2;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Statrted application");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[3]");
        sparkConf.setAppName("com.codesetters.Main");
        sparkConf.set("spark.cassandra.connection.host", "192.168.29.203");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Post> inputTableRdd = CassandraJavaUtil.javaFunctions(jsc)
                .cassandraTable("ritam", "post", CassandraJavaUtil.mapRowTo(Post.class))
                .rdd().toJavaRDD();
        inputTableRdd.cache();
        JavaPairRDD<String, Iterable<Post>> groupByPublisherName = inputTableRdd.groupBy(Post::getPublisher);
        JavaPairRDD<String, Integer> pairCount = groupByPublisherName.mapToPair(x -> new Tuple2<>(x._1(), Lists.newArrayList(x._2()).size()));
        JavaRDD<PublisherPostStats> s = pairCount.map(x -> new PublisherPostStats(LocalDate.now(), x._1(), x._2()));
        CassandraJavaUtil.javaFunctions(s).writerBuilder(
                "ritam",
                "publisherpoststats_date_bucket",
                CassandraJavaUtil.mapToRow(PublisherPostStats.class))
                .saveToCassandra();

    }
}
