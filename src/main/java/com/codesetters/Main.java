package com.codesetters;

import com.codesetters.domain.Post;
import com.codesetters.domain.PublisherPostStats;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Lists;
import scala.Tuple2;
import scala.Tuple3;

import java.time.LocalDate;
import java.time.ZoneId;

public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Statrted application");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[3]");
        sparkConf.setAppName("com.codesetters.Main");
        sparkConf.set("spark.cassandra.connection.host", "192.168.43.112");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Post> inputTableRdd = CassandraJavaUtil.javaFunctions(jsc)
                .cassandraTable("ritam", "post", CassandraJavaUtil.mapRowTo(Post.class))
                .rdd().toJavaRDD();
        System.out.println("total count");
        System.out.println(inputTableRdd.count());
        System.out.println("============cache=======");
        System.out.println(inputTableRdd.cache());
        inputTableRdd.cache();
        JavaRDD<Post> postJavaRDD = inputTableRdd.map(Main::mapTo);
        System.out.println("=========postjavardd==========");
        System.out.println(postJavaRDD.first());
        JavaPairRDD<Tuple3<String, LocalDate,String>, Iterable<Post>> groupByPublisherAndScrapedOn = postJavaRDD
                .groupBy(x -> new Tuple3<>(x.getPublisher(), x.getScrapedOn(),x.getPostType()));
        System.out.println("==============groupByPublisherAndScrapedOn========");
        System.out.println(groupByPublisherAndScrapedOn.first());
        JavaPairRDD<Tuple3<String, LocalDate,String>, Integer> countByPublisherNameAndDateAndType = groupByPublisherAndScrapedOn.mapToPair(x -> new Tuple2<>(x._1(), Lists.newArrayList(x._2()).size()));
        System.out.println("==========countByPublisherNameAndDate==============");
        System.out.println(countByPublisherNameAndDateAndType.first());
        JavaRDD<PublisherPostStats> publisherPostStatsJavaRDD = countByPublisherNameAndDateAndType
                .map(x -> new PublisherPostStats(x._1()._2(), x._1()._1(), x._2(),x._1._3()));
        System.out.println("============publisherPostStatsJavaRDD================");
        System.out.println(publisherPostStatsJavaRDD.first());

        CassandraJavaUtil.javaFunctions(publisherPostStatsJavaRDD).writerBuilder(
                "ritam",
                "publisherpoststats_date_bucket",
                CassandraJavaUtil.mapToRow(PublisherPostStats.class))
                .saveToCassandra();

    }

    private static Post mapTo(Post post) {
        if (post.getCreatedat() != null) {
            LocalDate scrapedOn = post.getCreatedat().toInstant()
                    .atZone(ZoneId.systemDefault()).toLocalDate();
            post.setScrapedOn(scrapedOn);
        }
        if(post.getPostType()== null) {
            post.setPostType("news");
        }
        return post;
    }
}
