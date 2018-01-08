package com.haben.pgreplication.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
public class MessageConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "test");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		Consumer<Long,String> consumer = new KafkaConsumer<Long, String>(props);

		consumer.subscribe(Arrays.asList("test"));

		System.out.println("开始的");
		while (true){
			try {
			ConsumerRecords<Long, String> poll = consumer.poll(1000);
				System.out.println(poll.count());
			poll.forEach((record)->{
				Long offset = record.key();
				String value = record.value();
				System.out.println(offset+"   "+value);
			});

				Thread.sleep(100);
			} catch (Exception e) {
//				consumer.seek();
//				consumer.se
				System.out.println("解析出错了"+e);
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
