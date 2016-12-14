package newborn_town.constant;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * @author chenhao
 * @version 创建时间：2016年8月2日 上午9:52:07
 */
public class KafkaConfig {

	public static Producer<String, String> getKafkaProducerConfiguration() {

		Properties props = new Properties();
		
		props.put("bootstrap.servers", Constant.KAFKA_HOST);
		props.put("acks", "all");// “所有”设置将导致记录的完整提交阻塞，最慢的，但最持久的设置。
		// 如果请求失败，生产者也会自动重试，即使设置成0 the producer can automatically retry.
		props.put("retries", 0);
		props.put("batch.size", Constant.KAFKA_BATCH_SIZE);
		props.put("linger.ms", 1);// 默认立即发送，这里这是延时毫秒数
		props.put("buffer.memory", Constant.KAFKA_BUFFER_MEMORY);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		return producer;
	}

	public static Properties getKafkaConsumerConfiguration() {

		Properties props = new Properties();
		
		props.put("bootstrap.servers", Constant.KAFKA_HOST);
		props.put("session.timeout.ms", Constant.KAFKA_SESSION_TIMEOUT);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		return props;

	}

}
