package com.github.charlesluxinger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ApiProducer {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var producer = new KafkaProducer<String, String>(properties());

		var key = UUID.randomUUID().toString();
		var value = key + ",321,456";
		var email = key + "Welcome to hell!";

		var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
		var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

		Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}

			System.out.println("::topic::" + data.topic() + "::partion::" + data.partition() + "::offset::" + data.offset() + "::timestamp::" + data.timestamp());
		};

		producer.send(record, callback).get();

		producer.send(emailRecord, callback).get();
	}

	private static Properties properties() {
		var properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}
}
