/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package poc.streaming.stream.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import poc.commons.SparkContextFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <topics> is a list of one or more kafka topics to consume from
 * <p>
 * Example:
 * $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 * topic1,topic2
 */

public final class JavaDirectKafkaWordCount {

    private static final String DEFAULT_TOPIC = "test";
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        final String topics = args.length == 1 ? args[0] : DEFAULT_TOPIC;

        KafkaConfig kafkaConfig = new KafkaConfig(topics);
        JavaStreamingContext streamContext = SparkContextFactory.createSimpleStreamingContext("local[2]", "JavaDirectKafkaWordCount");

        // Create direct kafka stream with brokers and topics
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(kafkaConfig.topicsSet, kafkaConfig.kafkaParams)
        );

        JavaPairDStream<String, String> messages = stream.mapToPair(
                (PairFunction<ConsumerRecord<String, String>, String, String>)
                        record -> {
                            System.out.println(record.key() + ":" + record.value());
                            return new Tuple2<>(record.key(), record.value());
                        }
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map((Function<Tuple2<String, String>, String>) tuple2 -> tuple2._2());

        JavaDStream<String> words =
                lines.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(SPACE.split(x))
                     .iterator());

        JavaPairDStream<String, Integer> wordCounts =
                words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                     .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        wordCounts.print();

        // Start the computation
        streamContext.start();
        streamContext.awaitTermination();
    }
}