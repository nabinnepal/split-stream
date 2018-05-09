package com.stream.kafka.example.runner;

import info.batey.kafka.unit.KafkaUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class SparkStreamRunnerTest {
    private static KafkaUnit kafkaUnitServer;
    private static JavaStreamingContext streamingContext;

    @BeforeClass
    public static void setup() throws IOException {
        kafkaUnitServer = new KafkaUnit();
        kafkaUnitServer.startup();
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming");
        streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
    }

    @AfterClass
    public static void tearDown() {
        streamingContext.stop();
        kafkaUnitServer.shutdown();
    }

    @Test
    public void getWordsCount() throws Exception {
        kafkaUnitServer.createTopic("test");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:" + kafkaUnitServer.getBrokerPort());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-group-id");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        SparkStreamRunner sparkStreamRunner = new SparkStreamRunner();
        JavaPairDStream<String, Long> sparkStream =
                sparkStreamRunner.getWordsCount(streamingContext, Arrays.asList("test"), kafkaParams);

        ArrayList<Tuple2<String, Long>> accumulator = new ArrayList<>();

        sparkStream.foreachRDD(rdd -> accumulator.addAll(rdd.collect()));

        streamingContext.start();

        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>("test", "key",
                "Multiple Multiple words");
        kafkaUnitServer.sendMessages(keyedMessage);
        Thread.sleep(2000);

        assertEquals(2, accumulator.size());
    }

}