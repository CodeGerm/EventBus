/**
 * 
 */
package org.cg.eventbus.producer;

import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.cg.eventbus.ConfigUtil;
import org.cg.eventbus.ICallback;
import org.cg.eventbus.ICallback.Response;
import org.cg.eventbus.IProducer;

/**
 * Producer using string key to partition
 * 
 * @author yanlinwang
 *
 */
public abstract class AbstractEventProducer<K, V> implements
		IProducer<K, V> {

	// Constants for producer

	public static final String DEFAULT_BROKER_LIST = "localhost:9092";

	public static final String PRO_TYPE_ASYNC = "async";

	public static final String NO_ACK = "0";
	public static final String LEADER_ACK = "1";
	public static final String REPLICAS_ACK = "-1";
	public static final String DEFAULT_PARTITION = "kafka.producer.DefaultPartitioner";

	private Logger logger = Logger
			.getLogger(AbstractEventProducer.class);

	/** topic name for this producer */
	private String topic;

	/** configuration for producer, */
	private Properties producerConfig;

	/* configuration passing along*/
	private Configuration config;
	
	/** producer */
	private KafkaProducer<K, V> producer;

	public AbstractEventProducer(String cfgPath) throws Exception {
		this(cfgPath, null);
	}
	

	public AbstractEventProducer(String cfgPath, String prefix) throws Exception {
		if (prefix==null)
			initialize (new PropertiesConfiguration(cfgPath));
		else
			initialize(ConfigUtil.extractConfiguration(ConfigurationConverter.getProperties(new PropertiesConfiguration(cfgPath)), prefix));
	}

	public AbstractEventProducer(Configuration config)
			throws Exception {
		initialize(config);
	}

	protected void initialize(Configuration config) throws Exception {
		this.config = config;
		ProducerConfigurator.validate(config);
		producerConfig = ConfigurationConverter.getProperties(config);
		topic = config.getString(PRODUCER_TOPIC);
		producer = new KafkaProducer<K, V>(producerConfig);
		logger.info("producer initialized");
	}

	
	
	/**
	 * @return the config
	 */
	public Configuration getConfig() {
		return config;
	}


	@Override
	public void send(V msg, final ICallback callback) {
		if (null == msg)
			return;
		K key = this.getKey(msg);

		ProducerRecord<K, V> data = new ProducerRecord<K, V>(topic, key, msg);
		producer.send(data, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				Response response = new Response ();
				response.setTopic(metadata.topic());
				response.setOffset(metadata.offset());
				response.setPartition(metadata.partition());
				if (callback != null)
					callback.onCompletion(response, e);
				if (e != null)
					logger.error("failed to send event, response = " + response.toString() , e );
			}
		});

	}

	@Override
	public void send(List<V> msgs, ICallback callback) {
		if (msgs==null) {
			logger.error("ignore null events");
			return;
		}
		for (V msg : msgs) {
			send (msg, callback);
		}
	}

	@Override
	public void close() {
		logger.info("Producer [" + topic + "] shutdown");
		producer.close();		
	}

}
