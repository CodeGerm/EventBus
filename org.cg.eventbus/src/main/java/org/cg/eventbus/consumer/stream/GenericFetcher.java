package org.cg.eventbus.consumer.stream;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.cg.eventbus.message.Message;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 *  Generic Kafka Consumer
 * @author liang.li
 *
 */
public class GenericFetcher {

	private static final Logger LOG = Logger.getLogger(GenericFetcher.class);
	
	private Configuration config;
	private ConsumerConnector consumerConnector;
	private ConsumerIterator<byte[], byte[]> consumerIter;
//	private String threadName;
	
	public GenericFetcher(Configuration conf) {
//		this.threadName = Thread.currentThread().getName();
		this.config = conf;
		init();
	}
	
	private void init() {
		ConsumerConfig consumerConf = new ConsumerConfigFactory()
				.setConfig(config)
				.build();
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConf);
		
		String topic = config.getString("consumer.topic");
		LOG.info(String.format("Thread get topic: %s", topic));
		TopicFilter topicFilter = new Whitelist(topic);
		List<KafkaStream<byte[], byte[]>> streams = consumerConnector
				.createMessageStreamsByFilter(topicFilter);
		KafkaStream<byte[], byte[]> stream = streams.get(0);
		consumerIter = stream.iterator();
	}
	
	public boolean hasNext() {
		return consumerIter.hasNext();
	}
	
	public Message read() {
		assert hasNext();
		MessageAndMetadata<byte[], byte[]> kafkaMessage = consumerIter.next();
		Message message = new Message(kafkaMessage.topic(),
				kafkaMessage.offset(), kafkaMessage.key(),
				kafkaMessage.message(), kafkaMessage.partition());
		
		return message;
	}
	
	public void commit() {
		consumerConnector.commitOffsets();
	}
	
	public void close() {
		consumerConnector.shutdown();
	}
}
