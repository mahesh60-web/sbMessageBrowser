package com.example.SBTopicTimetoProducerApplication;

import com.google.gson.Gson;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.ITopicClient;
import com.microsoft.azure.servicebus.Message;

import lombok.extern.log4j.Log4j2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;


@Log4j2
@Component
@RestController
class ServiceBusProducer {

    private final ITopicClient iTopicClient;
    private final Logger log = LoggerFactory.getLogger(ServiceBusProducer.class);
    static final Gson GSON = new Gson();
	
    ServiceBusProducer(ITopicClient iTopicClient) {
		this.iTopicClient = iTopicClient;
	}
    
    @SuppressWarnings("deprecation")
	@PostMapping("/messages")
    public void produce(@RequestParam String message) throws Exception {
    	Message message1 = new Message(message);
    	message1.setTimeToLive(Duration.ofMinutes(1));
		//message1.setScheduledEnqueuedTimeUtc(Clock.systemUTC().instant().plusSeconds(120));
		this.iTopicClient.send(message1);
    }
    
    @SuppressWarnings("serial")
	@PostMapping("/TimetoLive")
    public void producer(@RequestBody Order order) throws Exception {
    	final String messageId = Integer.toString(12);
    	
    	log.info("Color - " + order.getColor());
		log.info("Quantity - " + order.getQuantity());
		log.info("Priority - " + order.getPriority());
         
    	IMessage message = new Message(GSON.toJson(order, Order.class).getBytes(UTF_8));
        message.setContentType("application/json");
        message.setMessageId(messageId);
        message.setCorrelationId(order.getPriority());
        message.setLabel(order.getColor());
        message.setTimeToLive(Duration.ofMinutes(1));
        message.setProperties(new HashMap<String, Object>() {
        	{
            put("Color " , order.getColor());
            put("Quantity " , Integer.toString(order.getQuantity()));
            put("Priority " , order.getPriority());
            
        	}
		});
        
        iTopicClient.send(message);
    }
    
}
