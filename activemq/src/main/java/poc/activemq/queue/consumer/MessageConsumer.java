package poc.activemq.queue.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/**
 *
 */
@Component
public class MessageConsumer implements Consumer<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);


    public MessageConsumer(){
    }

    @Override
    public void accept(String message) {

        try {
            LOGGER.info("<==== Receiving message {}", message);
        } catch (Exception e) {
            LOGGER.error("Processing of the message has failed with Exception.", e);
        }
    }

}
