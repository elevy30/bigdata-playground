package poc.activemq.queue.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

@Slf4j
public class JavaActiveMQProducer implements ExceptionListener {

    private String brokerHost;
    private String user;
    private String password;

    private String queueNames;

    private Session session;
    private Destination destination;


//    private static int counter = 1; // a global counter
//    private static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy
//
//    private static void incrementCounter() {
//        counterLock.lock();
//        // Always good practice to enclose locks in a try-finally block
//        try {
//            System.out.println(Thread.currentThread().getName() + ": ####### CONSUME " + counter + " MESSAGES #######");
//            counter++;
//        } finally {
//            counterLock.unlock();
//        }
//    }

    public JavaActiveMQProducer(String brokerHost, String user, String password, String queueNames) throws JMSException {
        log.info("init JavaActiveMQProducer {}", brokerHost);
        // Clone is enough because Strings are immutable
        this.queueNames = queueNames;
        // Clone is enough because Strings are immutable
        this.brokerHost = brokerHost;
        this.user = user;
        this.password = password;

        init();
    }

    @SuppressWarnings("Duplicates")
    private void init() throws JMSException {
        // Create a ConnectionFactory
        log.info("{}", brokerHost);
        String retryConfiguration =  "";//"?maxReconnectAttempts=10&warnAfterReconnectAttempts=5";
//        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, "failover:(tcp://" + brokerHost + ")"+ retryConfiguration);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(tcp://" + brokerHost + ")"+ retryConfiguration);

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
        System.out.println("JMS Exception occured.  Shutting down client.");
    }

    void produce(String messageStr) throws JMSException {
        MessageProducer producer = null;
        try {
            log.info(String.format("Thread %s: Polling queue %s", Thread.currentThread().getName(), queueNames));
            // Create a MessageConsumer from the Session to the Topic or Queue
            producer = session.createProducer(destination);
            // producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //Now create the actual message you want to send
            TextMessage message = session.createTextMessage();
            message.setText(messageStr);

            producer.send(message);

            log.info("Disconnected from queue {}", queueNames);
        } catch (Exception e) {
            // Adding this general exception handler to avoid unexpected queue disconnections.
            log.error("Caught unexpected exception on queue {}: {}", queueNames, e.getMessage());
            e.printStackTrace();
            sleepAfterException(1000L);
//        } finally {
//            if (producer != null) {
//                producer.close();
//            }
        }
    }

    //    public void produceOneThread(String message) throws JMSException {
//        log.info("start consumer with one thread");
//
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        executor.submit(() -> {
//            try {
//                produce(message);
//            } catch (JMSException e) {
//                e.printStackTrace();
//            }
//        });
//    }
//
//    public void produceMultiThread(String message, int numOfThread) throws JMSException {
//        log.info("start consumer with multi thread");
//
//        ExecutorService executor = Executors.newFixedThreadPool(numOfThread);
//        executor.submit(() -> {
//            try {
//                produce(message);
//            } catch (JMSException e) {
//                e.printStackTrace();
//            }
//        });
//    }

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
