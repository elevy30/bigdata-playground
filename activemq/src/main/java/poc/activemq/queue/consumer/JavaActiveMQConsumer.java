package poc.activemq.queue.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.jms.MessageConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class JavaActiveMQConsumer implements ExceptionListener {

    private String brokerHost;
    private String user;
    private String password;

    private String queueNames;

    private Session session;
    private Destination destination;


    private static int counter = 1; // a global counter
    private static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy
    private static void incrementCounter(){
        counterLock.lock();
        // Always good practice to enclose locks in a try-finally block
        try{
            log.info(Thread.currentThread().getName() + ": ####### CONSUME " + counter + " MESSAGES #######");
            counter++;
        }finally{
            counterLock.unlock();
        }
    }

    public JavaActiveMQConsumer(String brokerHost, String user, String password, String queueNames) throws JMSException {
        log.info("init JavaActiveMQConsumer");
        // Clone is enough because Strings are immutable
        this.queueNames = queueNames;
        // Clone is enough because Strings are immutable
        this.brokerHost = brokerHost == null ? null : "localhost:61616";
        this.user = user;
        this.password = password;

        init();
    }

    @SuppressWarnings("Duplicates")
    private void init() throws JMSException {
        // Create a ConnectionFactory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "tcp://" + brokerHost + "?jms.prefetchPolicy.all=10&jms.optimizeAcknowledge=true");
        // ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "tcp://" + brokerHost + "?jms.prefetchPolicy.queuePrefetch=1");

        // Create a Connection
        Connection connection = connectionFactory.createConnection();
        connection.start();
        connection.setExceptionListener(this);

        // Create a Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        destination = session.createQueue(queueNames);
    }

    public synchronized void onException(JMSException ex) {
        log.info("JMS Exception occured.  Shutting down client.");
    }


    public void consumeOneThread() throws JMSException {
        log.info("start consumer with one thread");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> consume(session, destination));
    }

    public void consumeMultiThread(int numOfThread) throws JMSException {
        log.info("start consumer with multi thread");

        ExecutorService executor = Executors.newFixedThreadPool(numOfThread);
        executor.submit(() -> consume(session, destination));
    }


    private void consume(Session session, Destination destination) {
        try {
            log.info(String.format("Thread %s: Polling queue %s", Thread.currentThread().getName(), queueNames));
            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);

            do {
                // Wait for a message
                Message message = consumer.receiveNoWait();
                if(message != null) incrementCounter();
                printMsg(message);
            } while (!Thread.currentThread().isInterrupted());

            log.info("Disconnected from queue {}", queueNames);
        } catch (Exception e) {
            // Adding this general exception handler to avoid unexpected queue disconnections.
            log.error("Caught unexpected exception on queue {}: {}", queueNames, e.getMessage());
            e.printStackTrace();
            sleepAfterException(1000L);
        }
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
//                                            log.info(String.format("Thread %s: Polling queue %s", i, pair.getKey()));
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
//                                                log.error("Caught unexpected exception on queue {}: {}", pair.getKey(), e.getMessage());
//                                                e.printStackTrace();
//                                                sleepAfterException(1000L);
//                                            }
//                                            log.info("Disconnected from queue {}", pair.getKey());
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
                String id = text.substring(0, text.indexOf('_'));
                log.info("<====  Received: message with ID " + id);
            } else {
                log.error("Received: " + message);
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
