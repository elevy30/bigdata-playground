package poc.activemq.in.thetaray.queue.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import poc.activemq.in.thetaray.queue.connection.QueueConnectionSingleton;

import javax.jms.*;
import java.util.AbstractMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Created by eyallevy on 29/03/17
 */
@Slf4j
public class QueueConsumer {

    private String[] queueNames;
    private Integer threads;

    private Session session;

    public QueueConsumer(String[] queueNames, Integer threads, String[] brokerHosts, String user, String password) {
        // Clone is enough because Strings are immutable
        this.queueNames = queueNames == null ? null : queueNames.clone();
        this.threads = threads;
        // Clone is enough because Strings are immutable
        String[] brokerHostsClone = brokerHosts == null ? null : brokerHosts.clone();

        try {
            QueueConnectionSingleton instance = QueueConnectionSingleton.getInstance(brokerHostsClone, user, password);
            session = instance.getSession();
        } catch (Exception e) {
            //todo - handle exception
            log.error("JMS Exception occurred.  Shutting down client.", e);
        }
    }

    @Async
    public void daemon(final Consumer<String>... consumers) throws Exception {

//        for (int i = 0; i < queueNames.length; i++) {
//            //todo remove this loop
//            log.warn("0000000000000000000000000000 queue name {}, consumer = {}", queueNames[i], consumers[i].getClass().getSimpleName());
//        }

        if (queueNames == null || queueNames.length == 0) {
            throw new IllegalArgumentException("At least one queue must be provided");
        }
        if (queueNames.length != consumers.length) {
            throw new IllegalArgumentException("The number of consumers methods must match the number of queues");
        }

        // Create a future to wait for all queue polling threads
        ExecutorService executor = Executors.newFixedThreadPool(threads * consumers.length);
        //noinspection Duplicates
        CompletableFuture.allOf(
                // Create pairs of matching queue name and consumer method
                IntStream.range(0, queueNames.length)
                        .mapToObj(i -> new AbstractMap.SimpleEntry<>(queueNames[i], consumers[i]))
                        .collect(Collectors.toList())
                        .stream()
                        // For each pair create the requested threads
                        .flatMap(pair ->
                                // Create multiple threads
                                // First create a stream of indexes from 1 to the number of required threads
                                IntStream.rangeClosed(1, threads)
                                        // For each index create a future
                                        .mapToObj(aThread -> CompletableFuture.supplyAsync(() -> {
                                            String queueName = pair.getKey();

                                            log.info(String.format("Thread %s: Pushing queue %s", aThread, queueName));
                                            // Create a MessageConsumer from the Session to the Topic or Queue
                                            MessageConsumer consumer;
                                            try {
                                                consumer = session.createConsumer(session.createQueue(queueName));
                                                consumer.setMessageListener(message -> {
                                                    try {
                                                        collectMsg(pair, message);
                                                    } catch (JMSException e) {
                                                        log.error("Caught unexpected exception on queue {}: {}", queueName, e);
                                                    }
                                                });
                                            } catch (Exception e) {
                                                // Adding this general exception handler to avoid unexpected queue disconnections.
                                                log.error("Caught unexpected exception on queue {}: {}", queueName, e);
                                            }
                                            log.info("Consumer {} with Message Listener was registered to Queue {}",pair.getValue().getClass().getSimpleName(), queueName);

                                            return null;
                                        }, executor))
                        )
                        .toArray(CompletableFuture[]::new)
        )
                // Wait for the joined future to end
                .get();
    }

    private void collectMsg(AbstractMap.SimpleEntry<String, Consumer<String>> pair, Message message) throws JMSException {
        if (message != null) {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                pair.getValue().accept(textMessage.getText());
            }else{
                pair.getValue().accept("");
            }
        }
    }

}

