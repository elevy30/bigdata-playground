package poc.activemq.thetaray.queue.connection;


import lombok.extern.slf4j.Slf4j;
import poc.activemq.thetaray.queue.activemq.ActiveMQConnectionHelper;

import javax.jms.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by eyallevy on 29/03/17
 */

@Slf4j
public class QueueConnectionSingleton implements ExceptionListener{

    private static QueueConnectionSingleton instance = null;

    private String[] brokerHosts;
    private String user;
    private String password;

    private Session session;

    public static synchronized QueueConnectionSingleton getInstance(String[] brokerHosts, String user, String password) throws Exception {
        if(instance == null) {
            instance = new QueueConnectionSingleton(brokerHosts, user, password);
        }
        return instance;
    }

    private QueueConnectionSingleton(String[] brokerHosts, String user, String password) throws Exception {
        this.brokerHosts = brokerHosts;
        this.user = user;
        this.password = password;
        init();
    }

    private void init() throws Exception {
        // Create a ConnectionFactory
        String brokerHost = getMasterBroker(brokerHosts);

        // Create a Connection Factory
        QueueConnectionFactory connectionFactory = getQueueConnectionFactory(brokerHost);

        // Create a Connection
        Connection connection = connectionFactory.createConnection();
        connection.start();
        connection.setExceptionListener(this);

        // Create a Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private QueueConnectionFactory getQueueConnectionFactory(String brokerHost) {
        return ActiveMQConnectionHelper.getQueueConnectionFactory(brokerHost, user, password);
    }

    public synchronized void onException(JMSException ex) {
        log.error("JMS Exception occurred.  Shutting down client.", ex);
    }

    public Session getSession() {
        return session;
    }

    @SuppressWarnings("Duplicates")
    private String getMasterBroker(String[] brokerHosts) throws Exception {

        for ( String host : brokerHosts) {
            try {
                // Attempt to connect to the host:port, if successful this is the address of the active (master) broker
                String[] hostAndPort = host.split(":");
                InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                Socket socket = new Socket();
                socket.setReuseAddress(true);
                socket.connect(socketAddress);
                return host;
            } catch (IOException e) {
                // Try the next one, if array is exhausted an exception would be thrown
            }
        }
        throw new Exception("Can't find an active broker");
    }
}
