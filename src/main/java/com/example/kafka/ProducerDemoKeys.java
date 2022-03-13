package com.example.kafka;

import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ProducerDemoKeys {
	public static void main(String[] args) {
		String booststrapServers = "127.0.0.1:9092";
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

		// Create Producer properties
		Properties properties = new Properties();

		// Set properties
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, booststrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());;

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create  a producer record
		for (int i = 0; i < 10; i++) {
			
		
		ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic","hello word "+Integer.toString(i));
		
		// send data -asynchronous
		producer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executes everytime a record is successfully sent or an exception is thrown 
				
				if (exception == null) {
					// record was successfully sent
					logger.info("Received new metatdata \n" +
							"Topic:"+ metadata.topic()+"\n"+
							"Partition:"+metadata.partition()+"\n"+ 
							"Offset:"+metadata.offset()+"\n"+
							"Timestamp: "+metadata.timestamp());
				} else {
					logger.error("Error while producing",exception);
				}
				
			}
		});
		
		//flush data
		producer.flush();
		}
		
		//flush and close producer
		producer.close();
	}
}
