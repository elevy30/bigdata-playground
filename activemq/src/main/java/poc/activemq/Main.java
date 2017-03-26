package poc.activemq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.activemq.queue.consumer.HttpActiveMQConsumer;
import poc.activemq.queue.consumer.MessageConsumer;
import poc.activemq.queue.producer.MessageProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by eyallevy on 24/03/17 .
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static int PRODUCER_NUM_THREAD = 10;
    private static int CONSUMER_NUM_THREAD = 1;
    private static int NUM_OF_MSG = 1000;

    public static void main(String[] args) {
        MessageProducer producer = new MessageProducer(PRODUCER_NUM_THREAD);
        HttpActiveMQConsumer consumer = new HttpActiveMQConsumer(new String[]{"test"}, CONSUMER_NUM_THREAD, new String[]{"localhost:8161"}, "admin", "admin");

        StringBuilder messageBuilder = new StringBuilder("");
        for (int i = 1; i < 100; i++) {
            messageBuilder.append(",column_").append(i);
        }
        String message = messageBuilder.toString();

        runProducer(producer, message);
        runConsumer(consumer);

    }

    private static void runConsumer(HttpActiveMQConsumer consumer) {
        LOGGER.info("start consumer");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                consumer.daemon(new MessageConsumer());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void runProducer(MessageProducer producer, String message) {
        LOGGER.info("start producer");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> producer.send(message, NUM_OF_MSG));
    }
}
