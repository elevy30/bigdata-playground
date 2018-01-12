package poc.activemq.queue.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Active MQ broker failover implementation
 * User: sigals
 * Date: 29/07/2016
 */
@Slf4j
public class ActiveMQBrokerFailover {

    /**
     * Get the URL for an active MQ broker, except the queue name
     * @param url Active MQ API URL
     * @param user Active MQ user
     * @param password Active MQ user's password
     * @param brokerHosts Array of active MQ hosts (host:port)
     * @return Active MQ URL with the queue name not set (can be set with String.format)
     * @throws Exception If an active MQ broker is not found
     */
    public static String getMasterBrokerUrl(final String url, String user, String password, String[] brokerHosts) throws Exception {
        // The value of the queue name is not available, yet, replace it with another %s (otherwise the method throws
        // an exception)
        return String.format(url, user, password, getMasterBroker(brokerHosts), "%s");
    }

    /**
     * Get the URL for an active MQ broker
     * @param url Active MQ API URL
     * @param user Active MQ user
     * @param password Active MQ user's password
     * @param brokerHosts Array of active MQ hosts (host:port)
     * @param queueName Active MQ queue name
     * @return Active MQ URL
     * @throws Exception If an active MQ broker is not found
     */
    public static String getMasterBrokerUrl(final String url, String user, String password, String[] brokerHosts, final String queueName) throws Exception {
        return String.format(getMasterBrokerUrl(url, user, password, brokerHosts), queueName);
    }

    /**
     * Determine the active broker (master) from the list of addresses
     * @return Host and port (: as a delimiter) of the active (master) broker
     * @throws Exception If a live broker is not found
     * @param brokerHosts Array of active MQ broker hosts
     */
    private static String getMasterBroker(String[] brokerHosts) throws Exception {
    log.info("{}", brokerHosts);
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
