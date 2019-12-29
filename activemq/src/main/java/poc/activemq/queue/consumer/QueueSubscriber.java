package poc.activemq.queue.consumer;


import lombok.extern.slf4j.Slf4j;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

@Slf4j
public class QueueSubscriber {

    private String queueName = "test";
    private String initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
    private String connectionString = "tcp://admin:admin@0.0.0.0:61616";

    private boolean messageReceived = false;

    public static void main(String[] args) {
        QueueSubscriber subscriber = new QueueSubscriber();
        subscriber.subscribeWithQueueLookup();
    }

    public void subscribeWithQueueLookup() {

        Properties properties = new Properties();
        QueueConnection queueConnection = null;
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory", connectionString);
        properties.put("queue." + queueName, queueName);
        properties.put("username" , "admin");
        properties.put("password" , "admin");
        try {
            InitialContext ctx = new InitialContext(properties);
            QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
            queueConnection = queueConnectionFactory.createQueueConnection();
            System.out.println("Create Topic Connection for Topic " + queueName);

            while (true) {
                try {
                    QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

                    Queue queue = (Queue) ctx.lookup(queueName);
                    // start the connection
                    queueConnection.start();

                    // create a queue Receiver
                    javax.jms.QueueReceiver queueReceiver = queueSession.createReceiver(queue);

                    TestMessageListener messageListener = new TestMessageListener();
                    queueReceiver.setMessageListener(messageListener);

                    Thread.sleep(5000);
                    queueReceiver.close();
                    queueSession.close();
                } catch (JMSException | NamingException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (NamingException e) {
            throw new RuntimeException("Error in initial context lookup", e);
        } catch (JMSException e) {
            throw new RuntimeException("Error in JMS operations", e);
        } finally {
            if (queueConnection != null) {
                try {
                    queueConnection.close();
                } catch (JMSException e) {
                    throw new RuntimeException("Error in closing queue connection", e);
                }
            }
        }
    }

    public class TestMessageListener implements MessageListener {
        public void onMessage(Message message) {
            try {
                System.out.println("Got the Message : " + ((TextMessage) message).getText());
                messageReceived = true;
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

}

