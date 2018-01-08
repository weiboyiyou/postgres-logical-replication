package com.haben.pgreplication.kafka;

import com.haben.pgreplication.config.TaskConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-08 00:42
 * @Version: 1.0
 **/
public class MessageProducer {

	private Producer<Long, String> producer = null;

	public MessageProducer(TaskConfig config) {
		Properties props = new Properties();
		props.put("bootstrap.servers", config.getKafkaUrl());
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "1");
		producer = new KafkaProducer<Long, String>(props);
	}

	public void sendMsg(String topic, Long offset, String value) {
		ProducerRecord record = new ProducerRecord<>(topic, offset, value);
		producer.send(record);
		producer.flush();
		record = null;
	}



//	public KafkaAdminClient aa() {
//
//
//		return null;
//	}
//
//	public static void main(String[] args) {
////		Properties props = new Properties();
////		props.setProperty("bootstrap.servers","localhost:9092");
////		props.setProperty("serializer.class","kafka.serializer.StringEncoder");
////		props.put("request.required.acks","1");
//
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
//		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("acks", "1");
//
//		Producer<String, String> producer = new KafkaProducer<String, String>(props);
////		topictest.
//
//		for (int i = 0; i < 100; i++) {
//			producer.send(new ProducerRecord<String, String>("test", i + ":9999", "000000"));
//		}
//		producer.flush();
//		producer.close();
//
//		System.out.println("123123");
//
//	}
}
