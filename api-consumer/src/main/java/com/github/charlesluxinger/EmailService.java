package com.github.charlesluxinger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

	public static void main(String[] args) {
		var consumer = new KafkaConsumer<String, String>(properties());
		consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

		while (true){
			var records = consumer.poll(Duration.ofMillis(100));

			if (!records.isEmpty()){
				System.out.println("Found registry: " + records.count());
			}

			records.forEach(r -> {
				System.out.println("**************************");
				System.out.println("Send new email");
				System.out.println(r.key());
				System.out.println(r.value());
				System.out.println(r.partition());
				System.out.println(r.offset());

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				System.out.println("Email sent");
			});
		}

	}

	private static Properties properties() {
		var properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());

		return properties;
	}
}
