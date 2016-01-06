package org.cg.eventbus.consumer.bytestream;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.log4j.Logger;
import org.cg.eventbus.IEventBus;
import org.cg.eventbus.consumer.IConsumer;

import kafka.consumer.ConsumerConfig;

/**
 *  Building a consumer config factory,<br>
 *  using configuration, or building it step by step
 * @author liang.li
 *
 */

public class ConsumerConfigFactory {

	private Logger LOG = Logger.getLogger(ConsumerConfigFactory.class);
	private Properties consumerProp;
	
	public ConsumerConfigFactory() {
		consumerProp = new Properties();
	}
	
	public ConsumerConfig build() {
		validate();
		return new ConsumerConfig(consumerProp);
	}
	
	public ConsumerConfigFactory setConfig(Configuration config) {
		consumerProp = ConfigurationConverter.getProperties(config);
		return this;
	}
	
	public ConsumerConfigFactory setZookeeperConnection(String quorum) {
		consumerProp.put(IConsumer.ZK_CONNECT, quorum);
		return this;
	}
	
	//TODO
	
	private void validate(){
		if (!consumerProp.containsKey(IConsumer.GROUP_ID)) {
			LOG.error(IConsumer.GROUP_ID + " is missing");
			throw new IllegalArgumentException(IConsumer.GROUP_ID + " is missing");
		}
		
		if (!consumerProp.containsKey(IEventBus.ZK_CONNECT)) {
			throw new IllegalArgumentException("missing zookeeper connection.");
		}
	}
}
