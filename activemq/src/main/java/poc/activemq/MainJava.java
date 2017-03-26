package poc.activemq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.activemq.queue.consumer.JavaActiveMQConsumer;
import poc.activemq.queue.consumer.MessageConsumer;
import poc.activemq.queue.producer.MessageProducer;
import poc.activemq.queue.util.MessageBuilder;

import javax.jms.JMSException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by eyallevy on 24/03/17 .
 */
public class MainJava {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainJava.class);

    private static int PRODUCER_NUM_THREAD = 10;
    private static int CONSUMER_NUM_THREAD = 5;
    private static int NUM_OF_MSG = 100000;

    public static void main(String[] args) {
        try {
            String message = MessageBuilder.generateMsg();

            runProducer(message);
            runConsumer();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static void runProducer(String message) {
        LOGGER.info("start producer");
        MessageProducer producer = new MessageProducer(PRODUCER_NUM_THREAD);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> producer.send(message, NUM_OF_MSG));
    }

    private static void runConsumer() throws JMSException {
        LOGGER.info("start consumer");
        JavaActiveMQConsumer javaConsumer = new JavaActiveMQConsumer("localhost:61616","admin","admin", "test", 1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
//                consumer.daemon(new MessageConsumer());
                javaConsumer.consume();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
