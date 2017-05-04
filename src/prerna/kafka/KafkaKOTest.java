package prerna.kafka;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.instrument.Instrumentation;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.com.google.common.io.ByteStreams;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.FileHeader;

public class KafkaKOTest {

	private static final String FOLDER_TO_WATCH = "C:/Users/tbanach/Workspace/KOTestData";
	private static final String TOPIC = "testKey8";
	private static final int PARTITION = 0;
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String GROUP_ID = "myConsumerGroup";
	
	private final static char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
		
	public static void main(String[] args) throws Exception {
//		produce();
		consume();
	}
	
	private static void produce() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		// The hasher to manage creating the (almost) unique key
		// Probability of a collision is very, very small
		MessageDigest hasher = MessageDigest.getInstance("SHA-256");
		
		// Loop through the files and stream the records out
		File watchFile = new File(FOLDER_TO_WATCH);
		String[] zipFileNames = watchFile.list();	
		for (String zipFileName : zipFileNames) {
			ZipFile zipFile = new ZipFile(FOLDER_TO_WATCH + "/" + zipFileName);
			@SuppressWarnings("unchecked")
			List<FileHeader> fileHeaders = zipFile.getFileHeaders();
			for (FileHeader fileHeader : fileHeaders) {
				InputStream stream = zipFile.getInputStream(fileHeader);
				byte[] data = ByteStreams.toByteArray(stream);
				InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(data));
				BufferedReader bufferedReader = new BufferedReader(reader);
				String line;
				while((line = bufferedReader.readLine()) != null) {
					
					// Generate the (almost) unique key
					byte[] hash = hasher.digest(line.getBytes("UTF-8"));
					String key = bytesToHexString(hash);
					String[] tuple = line.split("\t");
					System.out.print("|");
					int width = 15;
					for (String element : tuple) {
						System.out.print(String.format("%1$" + width + "s", element + "|"));
					}
					System.out.println();
					ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, PARTITION, key, line);
					producer.send(record);
				}
			}
		}
		producer.close();
	}
	
	private static void consume() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("group.id", GROUP_ID);
		props.put("auto.offset.reset", "earliest");
		props.put("enable.auto.commit", "true");
//		props.put("fetch.message.max.bytes", "20");
//		props.put("max.partition.fetch.bytes", "20");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// List the partitions for the given topic
		List<PartitionInfo> parts = consumer.partitionsFor(TOPIC);
		for (int partIndex = 0; partIndex < parts.size(); partIndex++) {
			System.out.println("Partition for test " + parts.get(partIndex));
		}

		// Read the records
		TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION);
		consumer.assign(Arrays.asList(partition0));
		consumer.seekToBeginning(Arrays.asList(partition0));
		while (true) {
			System.out.println("Position... " + consumer.position(partition0));
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}
		}
	}
	
	public static String bytesToHexString(byte[] bytes) {
		
		// See http://stackoverflow.com/q/9655181
		char[] hexChars = new char[bytes.length * 2];
	    for (int i = 0; i < bytes.length; i++ ) {
	        int v = bytes[i] & 0xFF;
	        hexChars[i * 2] = HEX_ARRAY[v >>> 4];
	        hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
	    }
	    return new String(hexChars);
	}
		
}
