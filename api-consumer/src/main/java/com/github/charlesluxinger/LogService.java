package com.github.charlesluxinger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

	public static void main(String[] args) {
		var consumer = new KafkaConsumer<String, String>(properties());
		consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

		while (true){
			var records = consumer.poll(Duration.ofMillis(100));

			if (!records.isEmpty()){
				System.out.println("Found registry: " + records.count());
			}

			records.forEach(r -> {
				System.out.println("**************************");
				System.out.println("LOG");
				System.out.println(r.topic());
				System.out.println(r.key());
				System.out.println(r.value());
				System.out.println(r.partition());
				System.out.println(r.offset());
			});
		}

	}

	private static Properties properties() {
		var properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

		return properties;
	}
}
