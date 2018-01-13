package poc.platform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import poc.activemq.thetaray.queue.producer.QueueProducer;

@RestController
@EnableAutoConfiguration
@SpringBootApplication
public class Application {

    private QueueProducer queueProducer = new QueueProducer();

    @RequestMapping("/")
    public String home() {
        String msg =  "Hello Docker World";

        queueProducer.sendMessage(new String[] {"localhost:61616"}, "Admin", "Admin", msg, "AA_stage1");
        return msg;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}