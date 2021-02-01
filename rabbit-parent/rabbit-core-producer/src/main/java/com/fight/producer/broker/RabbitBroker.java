package com.fight.producer.broker;


import api.Message;

/**
 * 	$RabbitBroker 具体发送不同种类型消息的接口
 * @author Alienware
 *
 */
public interface RabbitBroker {
	
	void rapidSend(Message message);
	
	void confirmSend(Message message);
	
	void reliantSend(Message message);
	
	void sendMessages();
	
}
