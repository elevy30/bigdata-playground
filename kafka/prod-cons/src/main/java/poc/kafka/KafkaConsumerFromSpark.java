package poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by elevy on 20/12/16.
 */
public class KafkaConsumerFromSpark {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-local:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topicName = "profile";//"wikipedia-raw"

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        //consumer.subscribe(Arrays.asList("wikipedia-parsed"));
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        partitions.add(new TopicPartition(topicName, partitionInfos.get(0).partition()));

        List<Long> start = new ArrayList();
        List<Long> end = new ArrayList();

        System.out.println(partitionInfos);
        System.out.println(partitions);
        consumer.seekToBeginning(partitions);

        consumer.seekToBeginning(partitions);
        start.add(consumer.position(partitions.get(0)));
        System.out.println(start);

        consumer.seekToEnd(partitions);
        partitions.stream()
                .map(value -> consumer.position(value))
                .map(val -> end.add(val));

        Long sum = 0l;
        for (int i = 0; i < start.size(); i++) {
            Long l = end.get(i) - start.get(i);
            sum = +l;
        }

        System.out.println(sum);

        consumer.close();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
        }
    }
}
