package poc.activemq.queue.producer;

import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by eyallevy on 24/03/17
 */
@Slf4j
public class ProducerWrapper {

    private HttpActiveMQProducer httpActiveMQProducer;
    private JavaActiveMQProducer javaActiveMQProducer;
    private ExecutorService executor;

    private static long generatedID = 1; // a global counter
    private static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy
    private static void incrementId(){
        counterLock.lock();
        // Always good practice to enclose locks in a try-finally block
        try{
            generatedID++;
        }finally{
            counterLock.unlock();
        }
    }


    public ProducerWrapper(String brokerHost, String user, String password, String queueNames) throws JMSException {
        httpActiveMQProducer = new HttpActiveMQProducer();
        javaActiveMQProducer = new JavaActiveMQProducer(brokerHost, user, password, queueNames);
        executor = Executors.newFixedThreadPool(1);
    }

    public void send(String message, int numOfMsg, String taskOperator) {
        Callable<Long> task = sendWithHttp(message);
        if("java".equals(taskOperator)) {
            task = sendWithJava(message);
        }

        for (int i = 0; i < numOfMsg; i++) {
            Future<Long> future = executor.submit(task);
            log.debug("future done? " + future.isDone());
            Long result;
            try {
                result = future.get();
                log.debug("future done? " + future.isDone());
                log.debug("result: " + result);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private Callable<Long> sendWithJava(String message) {
        return () -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                String msg = generatedID + "___" + message;
                javaActiveMQProducer.produce(msg);
                log.info("====> Sent ==>  message with id {}", generatedID);
                incrementId();
                return generatedID;
            } catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
    }


    private Callable<Long> sendWithHttp(String message) {
        return () -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                String msg = generatedID + "___" + message;
                httpActiveMQProducer.sendToQueue(msg, "test");
                log.info("====> Sent ==>  message with id {}", generatedID);
                incrementId();
                return generatedID;
            } catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
    }
}
