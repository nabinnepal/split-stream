package com.stream.kafka.example.runner;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;


@Component
public class KafkaStreamRunner implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("test");
        textLines.flatMapValues(s -> Arrays.asList(s.split("\\W+"))).to("split");

        initStream(builder.build());
    }

    private void initStream(Topology topology) {
        Properties properties = new Properties();
        properties.put(APPLICATION_ID_CONFIG, "stream-example");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
