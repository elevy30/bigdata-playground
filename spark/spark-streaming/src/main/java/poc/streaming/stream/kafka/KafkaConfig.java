package poc.streaming.stream.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by elevy on 26/12/16.
 */
public class KafkaConfig {

    public Set<String> topicsSet;
    public Map<String, Object> kafkaParams;

    public KafkaConfig(String topics) {
        this.topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));;

        this.kafkaParams.put("bootstrap.servers", "localhost:9092");
        this.kafkaParams.put("key.deserializer", StringDeserializer.class);
        this.kafkaParams.put("value.deserializer", StringDeserializer.class);
        this.kafkaParams.put("group.id", "spark");
        this.kafkaParams.put("auto.offset.reset", "latest");
        this.kafkaParams.put("enable.auto.commit", false);
    }
}
