package poc.activemq.thetaray.queue.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


/**
 * Created by eyallevy on 29/03/17
 */

@Slf4j
@Service
public class QueueProducerBean {

    @Value("${broker.hosts}")
    private String[] brokerHosts;
    @Value("${broker.user}")
    private String user;
    @Value("${broker.password}")
    private String password;

    private QueueProducer queueProducer = new QueueProducer();

    public int sendToQueue(final String messageStr, final String queueName) {

        queueProducer.sendMessage(brokerHosts, user, password, messageStr, queueName);

        //todo - convert to void
        return 200;
    }
}
