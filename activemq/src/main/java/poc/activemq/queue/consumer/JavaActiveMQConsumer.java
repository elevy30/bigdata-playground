package poc.activemq.queue.consumer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import javax.jms.*;
import javax.jms.MessageConsumer;
import java.util.AbstractMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base class for active message queue consumer daemon
 * The consumer is based on the HTTP API to pop messages from a specific queue
 * This class will start a daemon process, which will start future(1) to consume messages from the queue.
 * To handle the messages the extending class must implement the method handleMessage
 * <p>
 * <br>
 * The following properties are required:
 * threads Number of threads to start per queue. If the property is not set 1 thread would be started
 * broker.user Active MQ user name
 * broker.password Active MQ user's password as plaintext
 * queue.name Comma separated list of Names of Active MQs queue to consume messages from
 * broker.hosts Comma separated list of Active MQ brokers, each broker must consist the host and port, e.g.
 * broker1:8161, broker2:8161
 * <p>
 * User: sigals
 * Date: 29/07/2016
 */
public class JavaActiveMQConsumer implements ExceptionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaActiveMQConsumer.class);

    private String brokerHost;
    private String user;
    private String password;

    private String queueNames;
    private Integer threads;

    private Connection connection;

    public JavaActiveMQConsumer(String brokerHost, String user, String password, String queueNames, Integer threads) throws JMSException {
        LOGGER.info("init JavaActiveMQConsumer");
        // Clone is enough because Strings are immutable
        this.queueNames = queueNames;
        this.threads = threads;
        // Clone is enough because Strings are immutable
        this.brokerHost = brokerHost == null ? null : "localhost:61616";
        this.user = user;
        this.password = password;

        init();
    }

    private void init() throws JMSException {
        // Create a ConnectionFactory
        // ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "tcp://" + brokerHost);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "tcp://" + brokerHost + "?jms.prefetchPolicy.all=10");
        // ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "tcp://" + brokerHost + "?jms.prefetchPolicy.queuePrefetch=1");

        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();
        connection.setExceptionListener(this);
    }

    public synchronized void onException(JMSException ex) {
        System.out.println("JMS Exception occured.  Shutting down client.");
    }


    public void consume() throws JMSException {
        LOGGER.info("start consumer");

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue(queueNames);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                LOGGER.info(String.format("Thread %s: Polling queue %s", Thread.currentThread().getName(), queueNames));
                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);
                do {
                    // Wait for a message
                    Message message = consumer.receiveNoWait();
                    printMsg(message);
                } while (!Thread.currentThread().isInterrupted());

                LOGGER.info("Disconnected from queue {}", queueNames);
            } catch (Exception e) {
                // Adding this general exception handler to avoid unexpected queue disconnections.
                LOGGER.error("Caught unexpected exception on queue {}: {}", queueNames, e.getMessage());
                e.printStackTrace();
                sleepAfterException(1000L);
            }
        });
    }

//    @Async
//    public void daemon(final Consumer<String>... consumers) throws Exception {
//        if (queueNames == null) {
//            throw new IllegalArgumentException("At least one queue must be provided");
//        }
////        if (queueNames.length != consumers.length) {
////            throw new IllegalArgumentException("The number of consumers methods must match the number of queues");
////        }
//
//        // Create a future to wait for all queue polling threads
//        ExecutorService executor = Executors.newFixedThreadPool(threads * consumers.length);
//        CompletableFuture.allOf(
//                // Create pairs of matching queue name and consumer method
//                IntStream.range(0, 1)
//                        .mapToObj(i -> new AbstractMap.SimpleEntry<>(queueNames, consumers[i]))
//                        .collect(Collectors.toList())
//                        .stream()
//                        // For each pair create the requested threads
//                        .flatMap(pair ->
//                                // Create multiple threads
//                                // First create a stream of indexes from 1 to the number of required threads
//                                IntStream.rangeClosed(1, threads)
//                                        // For each index create a future
//                                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
//                                            LOGGER.info(String.format("Thread %s: Polling queue %s", i, pair.getKey()));
//
//                                            // Create a MessageConsumer from the Session to the Topic or Queue
//                                            MessageConsumer consumer;
//                                            try {
//                                                consumer = session.createConsumer(destination);
//                                                do {
//                                                    Message message = consumer.receiveNoWait();
//                                                    printMsg(message);
//                                                    sleepAfterException(10L);
//                                                } while (!Thread.currentThread().isInterrupted());
//                                            } catch (Exception e) {
//                                                // Adding this general exception handler to avoid unexpected queue disconnections.
//                                                LOGGER.error("Caught unexpected exception on queue {}: {}", pair.getKey(), e.getMessage());
//                                                e.printStackTrace();
//                                                sleepAfterException(1000L);
//                                            }
//                                            LOGGER.info("Disconnected from queue {}", pair.getKey());
//
//                                            return null;
//                                        }, executor))
//                        )
//                        .toArray(CompletableFuture[]::new)
//        )
//                // Wait for the joined future to end
//                .get();
//    }

    private void printMsg(Message message) throws JMSException {
        if (message != null) {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                System.out.println("Received: " + text);
            } else {
                System.out.println("Received: " + message);
            }
        }
    }

    /**
     * Adds delay after an exception was caught.<BR>
     * In case of IO exception, it make sense to wait till IO error is resolved before retry.
     */
    private void sleepAfterException(long miliSec) {
        try {
            Thread.sleep(miliSec);
        } catch (InterruptedException e) { /* Squash shamelessly */ }
    }

}
