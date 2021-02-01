package com.fight.producer.broker;

import java.util.List;

import api.Message;
import api.MessageProducer;
import api.MessageType;
import api.SendCallback;
import api.exception.MessageRunTimeException;
import com.google.common.base.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * 	$ProducerClient 发送消息的实际实现类
 * @author Alienware
 *
 */
@Component
public class ProducerClient implements MessageProducer {
	@Autowired
	private RabbitBroker rabbitBroker;

	@Override
	public void send(Message message) throws MessageRunTimeException {
		Preconditions.checkNotNull(message.getTopic());
		String messageType = message.getMessageType();
		switch (messageType) {
			case MessageType.RAPID:
				rabbitBroker.rapidSend(message);
				break;
			case MessageType.CONFIRM:
				rabbitBroker.confirmSend(message);
				break;
			case MessageType.RELIANT:
				rabbitBroker.reliantSend(message);
				break;
			default:
				break;
		}

	}

	@Override
	public void send(List<Message> messages) throws MessageRunTimeException {

	}
	
	@Override
	public void send(Message message, SendCallback sendCallback) throws MessageRunTimeException {

	}

}
