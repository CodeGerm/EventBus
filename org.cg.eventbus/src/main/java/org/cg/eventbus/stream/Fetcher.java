package org.cg.eventbus.stream;

import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.log4j.Logger;
import org.cg.eventbus.consumer.ConsumerConfigrator;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

/**
 * 
 * @author liang.li
 *
 */
public class Fetcher<K, V> {
	
	private Logger LOG = Logger.getLogger(Fetcher.class);
	
	// 
	private final int STREAM_NUMBER = 1;
	
	private Configuration config;
	
	private ConsumerConnector connector;
	private KafkaStream<K, V> stream;
	private ConsumerIterator<K, V> consumerIter;
	
	public Fetcher(Configuration config) throws Exception {
		this.config = config;
		init();
	}
	
	@SuppressWarnings("unchecked")
	private void init() throws Exception {
		Properties prop = ConfigurationConverter.getProperties(config);
		ConsumerConfigrator.validate(prop);
		
		LOG.info("Consumer initialization started.");
		ConsumerConfig conf = new ConsumerConfig(prop);
		connector = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
		LOG.debug("Kafka consumer connector has been built.");
		
		String topic = config.getString("consumer.topic");
		TopicFilter filter = new Whitelist(topic);
		int threadNum = STREAM_NUMBER;
		
		Decoder<K> keyDecoder = ConsumerConfigrator.configKeyDecoder(prop);
		Decoder<V> valueDecoder = ConsumerConfigrator.configValueDecoder(prop);
		List<KafkaStream<K, V>> streams = connector.createMessageStreamsByFilter(filter, threadNum, keyDecoder, valueDecoder);
		stream = streams.get(0);
		
		consumerIter = stream.iterator();
	}
	
	public KafkaStream<K, V> getStream() {
		return stream;
	}
	
	public boolean hasNext() {
		return consumerIter.hasNext();
	}

	public void commit() {
		this.connector.commitOffsets(true);
	}
	
	public void close() {
		this.connector.shutdown();
	}

	public MessageAndMetadata<K, V> nextMessage() {
		return consumerIter.next();
	}
}
