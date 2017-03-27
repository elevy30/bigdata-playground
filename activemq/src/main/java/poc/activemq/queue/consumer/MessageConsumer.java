package poc.activemq.queue.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/**
 *
 */
@Component
@Slf4j
public class MessageConsumer implements Consumer<String> {

    public MessageConsumer(){
    }

    @Override
    public void accept(String message) {
        try {
            log.info("<==== Receiving message {}", message);
        } catch (Exception e) {
            log.error("Processing of the message has failed with Exception.", e);
        }
    }

}
