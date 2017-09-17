package com.ash.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MyKafkaConsumer {


	public static void main(String[] args) {

		Properties prop = new Properties();
		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181");
		prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		prop.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
		prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

		consumer.subscribe(Arrays.asList("ashtopic"));

		try{

			while(true){

				ConsumerRecords<String, String> records = consumer.poll(10);

				for(ConsumerRecord<String, String> record: records){

					System.out.printf("offset= %d; key: %s; value: %s", record.offset(),record.key(),record.value());

				}
			}}
		catch (KafkaException exception) {
			System.out.println("There was an exception: " + exception);
			System.out.println("");
		} 
		finally {
			consumer.close();
		} 


	}
}

