package poc.activemq.queue.util;

/**
 * Created by eyallevy on 26/03/17 .
 */
public class MessageBuilder {

    public static String generateMsg() {
        StringBuilder messageBuilder = new StringBuilder("");
        for (int i = 1; i < 100; i++) {
            messageBuilder.append(",column_\n").append(i);
        }
        return messageBuilder.toString();
    }
}
