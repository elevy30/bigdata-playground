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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import poc.commons.JavaSparkSessionSingleton;
import poc.commons.SparkContextFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Consumes messages from one or more topics in Kafka and do word count.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public final class StatefulKafkaToHDFS {

    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final String DEFAULT_TOPIC = "wikipedia-parsed";

    public static void main(String[] args) throws Exception {
        final String topics = args.length == 1 ? args[0] : DEFAULT_TOPIC;
        final String master = args.length == 2 ? args[1] : null;

        //#################################################################################################
        //init kafka & spark
        KafkaConfig kafkaConfig = new KafkaConfig(topics);
        JavaStreamingContext streamContext = SparkContextFactory.createSimpleStreamingContext(master, "StatefulKafkaToHDFS");

        //#################################################################################################
        // Create direct kafka stream with brokers and topics
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(kafkaConfig.topicsSet, kafkaConfig.kafkaParams)
        );

        //#################################################################################################
        //get topic and partition information
        stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
            final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition((VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });

        //#################################################################################################
        // parse json, get the byteDiff field, and return as a Tuple
        JavaPairDStream<String, Integer> messages = stream.mapToPair(
                (PairFunction<ConsumerRecord<String, String>, String, Integer>) rdd -> {
                    JsonParser parser = new JsonParser();
                    JsonObject rootObj = parser.parse(rdd.value()).getAsJsonObject();
                    String byteDiff = rootObj.get("byteDiff").getAsString();

                    System.out.println("2222222222222222222222222222222222222" + rootObj);
                    System.out.println("3333333333333333333333333333333333333" + byteDiff);

                    return new Tuple2<>("byteDiff", Integer.valueOf(byteDiff));
            }
        );


        //#################################################################################################
        // Initial state RDD input to mapWithState
        @SuppressWarnings("unchecked")
        List<Tuple2<String, Integer>> tuples =  Arrays.asList(new Tuple2<>("byteDiff", 0));
        JavaPairRDD<String, Integer> initialRDD = streamContext.sparkContext().parallelizePairs(tuples);

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = messages.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));


        //################################################################################################
        //Create JavaRDD<Row>
        stateDstream.foreachRDD( (JavaRDD<Tuple2<String, Integer>> rdd) -> {
            JavaRDD<Row> rowRDD = rdd.map( msg ->  RowFactory.create(msg._1, msg._2));
                JavaRDD<Row> repartition = rowRDD.repartition(1);

                //Create Schema
                StructField byteDiff = DataTypes.createStructField("byteDiff", DataTypes.StringType, true);
                StructField numberOfByte = DataTypes.createStructField("numberOfByte", DataTypes.IntegerType, true);
                StructType schema = DataTypes.createStructType(new StructField[] {byteDiff, numberOfByte});

                //Get Spark 2.0 session
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                //Create DataFrame
                Dataset<Row> msgDataFrame = spark.createDataFrame(repartition, schema);
                msgDataFrame.show();
                msgDataFrame.write().mode(SaveMode.Append).option("header", "true").csv("hdfs://0.0.0.0:9000/kafkaspark/byteDiff.csv");
                msgDataFrame.printSchema();
            });

        stateDstream.print();
        //stateDstream.dstream().repartition(1).saveAsTextFiles("hdfs://0.0.0.0:9000/kafkaspark​/wiki","txt");


        //################################################################################################
        // Start the computation
        streamContext.start();
//      SQLContext sqlContext = new SQLContext(streamContext.sparkContext());
//      DataFrameReader dataFrameReader = sqlContext.read();
        streamContext.awaitTermination();
    }
}