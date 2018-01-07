package com.haben.pgreplication.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Haben
 * @Description:
 * @Date: 2018-01-08 01:10
 * @Version: 1.0
 **/
public class KafkaConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "test");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		Consumer<String,String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);

		consumer.subscribe(Arrays.asList("topictest","test"));

		System.out.println("开始的");
		while (true && 1==1){
			ConsumerRecords<String, String> poll = consumer.poll(1000);
			poll.forEach((record)->{
				String key = record.key();
				String value = record.value();
				System.out.println(key+"   "+value);

			});
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
//		System.out.println("打印的");
//
//
//		Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
//		stringListMap.forEach((k,v)->{
//			System.out.println(k+" + "+v);
//		});
//
//		System.out.println("123123");
	}
}
