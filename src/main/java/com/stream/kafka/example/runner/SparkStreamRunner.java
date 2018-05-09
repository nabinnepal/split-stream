package com.stream.kafka.example.runner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SparkStreamRunner implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-group-id");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("test");

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaPairDStream<String, Long> wordsCount1 = getWordsCount(streamingContext,
                Collections.singletonList("test"),
                kafkaParams);

        wordsCount1.print();

        wordsCount1.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            ((CanCommitOffsets) wordsCount1).commitAsync(offsetRanges);
        });
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public JavaPairDStream<String, Long> getWordsCount(JavaStreamingContext streamingContext,
                                                        List<String> topics,
                                                        Map<String, Object> kafkaParams) {
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String, Long> wordsCount = stream
                .flatMap(cr -> Arrays.asList(cr.value().split("\\s")).iterator())
                .countByValue();



//        JavaPairRDD<String, Long> result = wordsCount.compute(Time.apply(1000));

        return wordsCount;
    }
}
