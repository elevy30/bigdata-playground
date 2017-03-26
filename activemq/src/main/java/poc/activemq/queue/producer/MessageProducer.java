package poc.activemq.queue.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by eyallevy on 24/03/17
 */
public class MessageProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    private ActiveMQProducerController mqProducerController;
    private ExecutorService executor;

    public MessageProducer(int producerNumThread) {
        mqProducerController = new ActiveMQProducerController();
        executor = Executors.newFixedThreadPool(producerNumThread);
    }

    public void send(String message, int numOfMsg) {
        Callable<Long> task = () -> {
            try {
                long id = System.currentTimeMillis();
                TimeUnit.MILLISECONDS.sleep(1);
                String msg = id + "___" + message;
                //LOGGER.info("====> Sending message {}", msg);
                mqProducerController.sendToQueue(msg, "test");
                return id;
            } catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };

        for (int i = 0; i < numOfMsg; i++) {
            Future<Long> future = executor.submit(task);
            //System.out.println("future done? " + future.isDone());
            Long result = null;
            try {
                result = future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            //LOGGER.info("future done? " + future.isDone());
            //LOGGER.info("result: " + result);
        }
    }
}
