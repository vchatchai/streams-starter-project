package com.github.simple.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamStartApp {

    public static void main(String[] args) {
        Properties  config  = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:7092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> wordCountInput  = builder.stream("word-count-input");

        KTable<String,Long> wordCounts =  wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((ignoredKey, word) ->  word)
                .groupByKey()
                .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        CountDownLatch latch  = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("word-count-output"){
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {

            streams.start();
            System.out.println(streams.toString());
            latch.await();
        } catch (final Throwable e){
            System.exit(1);
        }

        System.exit(0);

    }
}
