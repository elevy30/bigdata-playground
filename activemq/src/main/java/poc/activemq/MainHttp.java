package poc.activemq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.activemq.queue.consumer.HttpActiveMQConsumer;
import poc.activemq.queue.consumer.MessageConsumer;
import poc.activemq.queue.producer.MessageProducer;
import poc.activemq.queue.util.MessageBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by eyallevy on 24/03/17 .
 */
public class MainHttp {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainHttp.class);

    private static int PRODUCER_NUM_THREAD = 10;
    private static int CONSUMER_NUM_THREAD = 1;
    private static int NUM_OF_MSG = 1000;

    public static void main(String[] args) {
        String message = MessageBuilder.generateMsg();

        runProducer(message);
        runConsumer();
    }

    private static void runConsumer() {
        LOGGER.info("start consumer");
        HttpActiveMQConsumer consumer = new HttpActiveMQConsumer(new String[]{"test"}, CONSUMER_NUM_THREAD, new String[]{"localhost:8161"}, "admin", "admin");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                consumer.daemon(new MessageConsumer());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void runProducer( String message) {
        LOGGER.info("start producer");
        MessageProducer producer = new MessageProducer(PRODUCER_NUM_THREAD);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> producer.send(message, NUM_OF_MSG));
    }
}
