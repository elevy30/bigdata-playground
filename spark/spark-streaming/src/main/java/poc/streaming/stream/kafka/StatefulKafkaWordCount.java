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
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import poc.commons.SparkContextFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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

public final class StatefulKafkaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    private static final String DEFAULT_TOPIC = "wikipedia-parsed";

    public static void main(String[] args) throws Exception {

        final String topics = args.length == 1 ? args[0] : DEFAULT_TOPIC;
        final String master = args.length == 2 ? args[1] : null;

        //#################################################################################################
        //init kafka & spark
        KafkaConfig kafkaConfig = new KafkaConfig(topics);
        JavaStreamingContext streamContext = SparkContextFactory.createSimpleStreamingContext(master, "StatefulKafkaToHDFS");


        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")
        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = streamContext.sparkContext().parallelizePairs(tuples);

        // Create direct kafka stream with brokers and topics
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(kafkaConfig.topicsSet, kafkaConfig.kafkaParams)
        );

        stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
            final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition((VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });

        JavaPairDStream<String, String> messages1 = stream.mapToPair(
                (PairFunction<ConsumerRecord<String, String>, String, String>)
                        record -> {
                            System.out.println(record.key() + ":" + record.value());
                            return new Tuple2<>(record.key(), record.value());
                        }
        );

        JavaPairDStream<String, String> filters = messages1.filter(tuple2 -> new Boolean(tuple2._2.contains("AvicBot")));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = filters.map((Function<Tuple2<String, String>, String>) tuple2 -> tuple2._2());

        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordsDStream = words.mapToPair(s -> new Tuple2<>(s, 1));

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDStream = wordsDStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));


        stateDStream.print();

        // Start the computation
        streamContext.start();
        streamContext.awaitTermination();
    }
}