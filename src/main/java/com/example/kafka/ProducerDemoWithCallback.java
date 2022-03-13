package com.example.kafka;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
public class ProducerDemoWithCallback {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String booststrapServers = "127.0.0.1:9092";
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
			
			String topic = "first_topic";
			String value = "hello wrld"+Integer.toString(i);
			String key = "id_"+Integer.toString(i);
		
		ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, key, value);
		
		logger.info("Key" +key); //log the key 
		//id_0 - 1 
		//id_1 - 0
		//id_2 - 2 
		//id_3 - 0
		//id_4 - 2
		//id_5 - 2
		//id_6 - 0
		//id_7 - 2
		//id_8 - 1
		//id_9 - 2
		
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
		}).get(); // block the .send to make it synchornous - not production adviseable 
		
		//flush data
		producer.flush();
		}
		
		//flush and close producer
		producer.close();
	}
}
