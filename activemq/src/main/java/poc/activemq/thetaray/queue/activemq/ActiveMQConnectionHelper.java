package poc.activemq.thetaray.queue.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.QueueConnectionFactory;

/**
 * Created by eyallevy on 01/04/17
 */
public class ActiveMQConnectionHelper {

    public static QueueConnectionFactory getQueueConnectionFactory(String brokerHost, String user, String password) {
        String retryConfiguration =  "?maxReconnectAttempts=10&warnAfterReconnectAttempts=5";
        return new ActiveMQConnectionFactory(user, password, "failover:(tcp://" + brokerHost + ")" + retryConfiguration);
    }
}
