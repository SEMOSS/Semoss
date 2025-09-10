package prerna.logging;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaLogger implements IQueueLogger {

	private volatile KafkaProducer<String, String> producer;
	private final Properties config;
	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private ScheduledFuture<?> kafkaCheckTask;

	public KafkaLogger(String config) {
		this.config = defaultProps(config);
	}

	@Override
	public void init() {
		startKafkaMonitor();
	}

	private void startKafkaMonitor() {
		kafkaCheckTask = scheduler.scheduleAtFixedRate(() -> {
			if (isKafkaUp(config.getProperty("bootstrap.servers"))) {
				if (producer == null) {
					getOrCreateProducer();
				}
				// Kafka is up - stop scheduler
				stopKafkaMonitor();
			}

		}, 0, 10, TimeUnit.SECONDS);

	}

	// Monitor will be stopped(kafka is up)
	private void stopKafkaMonitor() {
		if (kafkaCheckTask != null && !kafkaCheckTask.isCancelled()) {
			kafkaCheckTask.cancel(false);
		}

	}

	private Producer<String, String> getOrCreateProducer() {
		try {
			if (producer == null) {
				if (isKafkaUp(config.getProperty("bootstrap.servers"))) {
					synchronized (this) {
						if (producer == null) {
							producer = new KafkaProducer<>(config);
						}
					}

				}
			}
		} catch (Exception ex) {
			System.err.println("Failed to initialize KafkaProducer: " + ex.getMessage());
		}

		return producer;
	}

	@Override
	public void send(String key, String value) {
		Producer<String, String> kafkaProducer = getOrCreateProducer();
		if (kafkaProducer != null && isKafkaUp(config.getProperty("bootstrap.servers"))) {
			try {

				String topic = config.getProperty("topic");
				ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
				producer.send(record, (metadata, ex) -> {
					if (ex != null) {
						System.err.println("Failed to send log to Kafka: " + ex.getMessage());
					}
				});
				// System.out.println(send.get());
			} catch (Exception ex) {
				System.err.println("Exception while sending message: " + ex.getMessage());
			}
		} else {
			System.err.println("Kafka is down");
		}
	}

	@Override
	public void close() {
		producer.close();
	}

	private Properties defaultProps(String config) {
		Properties props = getProperties(config);
		props.put(ProducerConfig.ACKS_CONFIG, "0");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return props;
	}

	private boolean isKafkaUp(String bootStrapServers) {
		try (AdminClient adminClient = AdminClient.create(kafkaProperties(bootStrapServers))) {
			adminClient.listTopics(new ListTopicsOptions().timeoutMs(2000)).names().get();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private Properties kafkaProperties(String bootStrapServers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootStrapServers);
		return props;
	}

}
