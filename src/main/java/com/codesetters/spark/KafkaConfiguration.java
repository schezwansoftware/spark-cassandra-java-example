package com.codesetters.spark;


import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class KafkaConfiguration {
      private String sync;
      private String topic;
      private Producer<String, String> producer;
      SparkStreamingConfig sparkStreamingConfig;

      public void producer() {
          System.out.println("==============called========");
          sparkStreamingConfig = new SparkStreamingConfig();
          String key = "Key1";
          String value = "Value-1";
          Map<String,Object> kafkaParams = new HashMap<String,Object>();
          kafkaParams.put("bootstrap.servers", "192.168.43.112:9092");
          kafkaParams.put("key.serializer", StringSerializer.class);
          kafkaParams.put("value.serializer", StringSerializer.class);
          kafkaParams.put("group.id", "group_json");
          kafkaParams.put("auto.offset.reset", "latest");
          kafkaParams.put("enable.auto.commit", false);
          kafkaParams.put("advertised.host.name","192.168.43.112");
          kafkaParams.put("advertised.port","9092");
//          Producer<String, String> producer = new KafkaProducer<>(kafkaParams);
          Collection<String> topics = Arrays.asList("mytopic,baggu");
          JavaInputDStream<ConsumerRecord<String, String>> stream =
                  KafkaUtils.createDirectStream(
                          sparkStreamingConfig.connect(),
                          LocationStrategies.PreferConsistent(),
                          ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                  );
          System.out.println();
          System.out.println("===========staream count========");
          System.out.println(stream.count());
          stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
      }
}
