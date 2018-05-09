package com.stream.kafka.example.runner;

import com.stream.kafka.example.logs.ApacheAccessLog;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.CommandLineRunner;

public class ApacheLogStreamRunner implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaDStream<String> accessLogStream = streamingContext.textFileStream("/tmp/logs");

        JavaDStream<ApacheAccessLog> accessLogJavaDStream = accessLogStream.map(this::mapToAccessLog);

    }

    private ApacheAccessLog mapToAccessLog(String logLine) {
        return ApacheAccessLog.parseFromLogLine(logLine);
    }
}
