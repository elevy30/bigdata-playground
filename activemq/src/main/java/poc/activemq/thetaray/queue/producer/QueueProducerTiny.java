package poc.activemq.thetaray.queue.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;

/**
 * Created on 9/27/16
 */
@Slf4j
public class QueueProducerTiny implements Serializable, ExceptionListener{

    private static final String RETRY_CONFIG =  "?maxReconnectAttempts=10&warnAfterReconnectAttempts=5";
    private String brokerHost;
    private String user;
    private String password;

    public QueueProducerTiny(String brokerHost, String user, String password) {
        this.brokerHost = brokerHost;
        this.user = user;
        this.password = password;
    }

    @SuppressWarnings("Duplicates")
    public void sendMessage(final String messageStr, final String queueName) {
        QueueConnectionFactory connectionFactory;
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            // Create a ConnectionFactory
            // String brokerHost = getMasterBroker(brokerHosts);

            // Create a Connection Factory
            log.debug("Create Connection Factory for QUEUE:{}", queueName);
            connectionFactory = new ActiveMQConnectionFactory(user, password, "failover:(tcp://tr-activemq:61616)" + RETRY_CONFIG);

            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(this);

            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queueName);
            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session.createProducer(destination);
            // producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //Now create the actual message you want to send
            TextMessage message = session.createTextMessage();
            message.setText(messageStr);

            log.debug("Send Message to QUEUE:{}", queueName);
            producer.send(message);
        } catch (Exception e) {
            log.error("Caught unexpected exception when trying to send message to queue {}", queueName, e);
        } finally {
            //noinspection Duplicates
            try {
                if (producer != null) {
                    producer.close();
                    log.debug("## Producer ## for queue {} was closed", queueName);
                }
                if(session != null) {
                    session.close();
                    log.debug("## Session ## for queue {} was closed", queueName);
                }
                if(connection != null) {
                    connection.close();
                    log.debug("## Connection ## for queue {} was closed", queueName);
                }
                connectionFactory = null;
            } catch (JMSException e) {
                log.error("Caught unexpected exception when trying to close producer/session (on queue {})", queueName, e);
            }
        }
    }
    public synchronized void onException(JMSException ex) {
        log.error("JMS Exception occurred.  Shutting down client.", ex);
    }

}
