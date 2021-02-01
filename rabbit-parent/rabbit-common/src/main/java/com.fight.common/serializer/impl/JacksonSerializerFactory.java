package com.fight.common.serializer.impl;


import api.Message;
import com.fight.common.serializer.Serializer;
import com.fight.common.serializer.SerializerFactory;



public class JacksonSerializerFactory implements SerializerFactory {

	public static final SerializerFactory INSTANCE = new JacksonSerializerFactory();

	@Override
	public Serializer create() {
		return JacksonSerializer.createParametricType(Message.class);
	}

}
