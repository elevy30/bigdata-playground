package poc.activemq.queue.consumer;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import poc.activemq.queue.ActiveMQBrokerFailover;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class HttpActiveMQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpActiveMQConsumer.class);

    private static final String BROKER_URL_FORMAT = "http://%s:%s@%s/api/message?destination=queue://%s&oneShot=true&jms.prefetchPolicy.all=50";

    private String[] queueNames;
    private Integer threads;
    private String[] brokerHosts;
    private String user;
    private String password;

    public HttpActiveMQConsumer(String[] queueNames, Integer threads, String[] brokerHosts, String user, String password) {
        LOGGER.info("init HttpActiveMQConsumer");
        // Clone is enough because Strings are immutable
        this.queueNames = queueNames == null ? null : queueNames.clone();
        this.threads = threads;
        // Clone is enough because Strings are immutable
        this.brokerHosts = brokerHosts == null ? null : brokerHosts.clone();
        this.user = user;
        this.password = password;
    }

    @Async
    public void daemon(final Consumer<String>... consumers) throws Exception {

        if (queueNames == null || queueNames.length == 0) {
            throw new IllegalArgumentException("At least one queue must be provided");
        }
        if (queueNames.length != consumers.length) {
            throw new IllegalArgumentException("The number of consumers methods must match the number of queues");
        }

        // Create a future to wait for all queue polling threads
        ExecutorService executor = Executors.newFixedThreadPool(threads * consumers.length);
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
                                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> {

                                            String brokerUri;
                                            try {
                                                brokerUri =
                                                        ActiveMQBrokerFailover.getMasterBrokerUrl(BROKER_URL_FORMAT, user, password, brokerHosts, pair.getKey());
                                            } catch (Exception e) {
                                                // Can't get a broker URI, stop the future
                                                LOGGER.error("Failed to identify the master broker, exiting...", e);
                                                return null;
                                            }

                                            LOGGER.info(String.format("Thread %s: Polling queue %s", i, pair.getKey()));

                                            // The URL to consume messages from the queue, hangs until a message is available,
                                            // unless it timeouts. In any case we'd like to call the URL again to get the next
                                            // message
                                            do {

                                                HttpClient client = HttpClientBuilder.create().build();
                                                HttpGet get = new HttpGet(brokerUri);

                                                try {
                                                    HttpResponse response = client.execute(get);
                                                    int statusCode = response.getStatusLine().getStatusCode();
                                                    if (statusCode == HttpStatus.SC_OK) {

                                                        // Create a string from the result of the consumption call, and send to
                                                        // handling
                                                        InputStream is = response.getEntity().getContent();
                                                        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
                                                            String collect = buffer.lines().collect(Collectors.joining("\n"));
                                                            LOGGER.info(collect);
                                                            pair.getValue().accept(collect);
                                                        } finally {
                                                            try {
                                                                is.close();
                                                            } catch (IOException e) {
                                                                LOGGER.error("Unable to read data", e);
                                                            }
                                                        }
                                                    } else {
                                                        if (statusCode != HttpStatus.SC_NO_CONTENT) {
                                                            LOGGER.info(response.getStatusLine().getReasonPhrase());
//                                                            LOGGER.info(response.getReasonPhrase());
                                                            LOGGER.info("Couldn't get content from queue, status code on queue {}: {}", pair.getKey(), statusCode);
                                                        }
                                                    }
                                                } catch (NoHttpResponseException e) {
                                                    try {
                                                        // If there's only one host, there's no failover
                                                        if (brokerHosts.length != 1) {
                                                            brokerUri =
                                                                    ActiveMQBrokerFailover.getMasterBrokerUrl(
                                                                            BROKER_URL_FORMAT, user, password, brokerHosts, pair.getKey());
                                                        }
                                                    } catch (Exception e1) {
                                                        if (LOGGER.isTraceEnabled()) {
                                                            LOGGER.trace("Failed to identify the master broker", e);
                                                        }
                                                    }
                                                } catch (IOException e) {
                                                    LOGGER.error("Unable to read data from queue {}: {}", pair.getKey(), e.getMessage());
                                                    sleepAfterException(1000L);
                                                } catch (Exception e) {
                                                    // Adding this general exception handler to avoid unexpected queue disconnections.
                                                    LOGGER.error("Caught unexpected exception on queue {}: {}", pair.getKey(), e.getMessage());
                                                    sleepAfterException(2000L);
                                                }

                                            } while (!Thread.currentThread().isInterrupted());

                                            LOGGER.info("Disconnected from queue {}", pair.getKey());

                                            return null;
                                        }, executor))
                        )
                        .toArray(CompletableFuture[]::new)
        )
                // Wait for the joined future to end
                .get();
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
