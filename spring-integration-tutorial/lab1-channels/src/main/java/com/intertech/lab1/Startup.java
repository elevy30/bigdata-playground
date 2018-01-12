package com.intertech.lab1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

@Slf4j
public class Startup {

	@SuppressWarnings({ "resource", "unused" })
	// @SuppressWarnings({ "resource" })
	public static void main(String[] args) {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/META-INF/spring/si-components.xml");
		while (true) {

			MessageChannel channel = context.getBean("messageChannel", MessageChannel.class);
			Message<String> message1 = MessageBuilder.withPayload("Hello world - one!").build();
			Message<String> message2 = MessageBuilder.withPayload("Hello world - two!").build();
			Message<String> message3 = MessageBuilder.withPayload("Hello world - three!").build();

			log.info("sending message1");
			channel.send(message1);
			log.info("sending message2");
			channel.send(message2);
			log.info("sending message3");
			channel.send(message3);
			log.info("done sending messages");
		}
	}
}
