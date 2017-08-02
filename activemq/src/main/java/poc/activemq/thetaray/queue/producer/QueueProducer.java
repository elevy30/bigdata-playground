package poc.activemq.thetaray.queue.producer;

import lombok.extern.slf4j.Slf4j;
import poc.activemq.thetaray.queue.connection.QueueConnectionSingleton;

import javax.jms.*;
import java.io.Serializable;

/**
 * Created by eyallevy on 01/04/17
 */
@Slf4j
public class QueueProducer implements Serializable {

    public void sendMessage(String[] brokerHosts, String user, String password, final String messageStr, final String queueName) {
        MessageProducer producer = null;
        Session session = null;
        try {
            session = QueueConnectionSingleton.getInstance(brokerHosts, user, password).getSession();

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queueName);
            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session.createProducer(destination);
            // producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //Now create the actual message you want to send
            TextMessage message = session.createTextMessage();
            message.setText(messageStr);

            producer.send(message);
        } catch (Exception e) {
            log.error("Caught unexpected exception when trying to send message to queue {}", queueName, e);
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                    log.debug("producer for queue {} was closed", queueName);
                }
//                if(session != null) {
//                    session.close();
//                    log.debug("session for queue {} was closed", queueName);
//                }
            } catch (JMSException e) {
                log.error("Caught unexpected exception when trying to close producer/session (on queue {})", queueName, e);
            }
        }
    }
}
