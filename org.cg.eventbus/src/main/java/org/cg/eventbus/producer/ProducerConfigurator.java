/**
 * 
 */
package org.cg.eventbus.producer;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.cg.eventbus.EventBusManager;
import org.cg.eventbus.IProducer;

/**
 * @author yanlinwang
 *
 */
public class ProducerConfigurator {

	private static Logger logger = Logger.getLogger(ProducerConfigurator.class);

	/**
	 * validate required configuration zk.connect: zookeeper connection
	 * metadata.broker.list: kafka broker list producer.topic: topic name this
	 * producer sending to
	 * 
	 * @param config
	 * @throws Exception
	 */
	public synchronized static void validate(Configuration config)
			throws Exception {

		if (!config.containsKey(IProducer.BROKER_LIST)) {
			throw new IllegalArgumentException("Missing configuration "
					+ IProducer.BROKER_LIST);
		}
		logger.info("EventBus  broker list: "
				+ config.getString(IProducer.BROKER_LIST));
		String brokerList = config.getString(IProducer.BROKER_LIST);
		if (null == brokerList || brokerList.isEmpty()) {
			throw new IllegalArgumentException("Missing configuration "
					+ IProducer.BROKER_LIST);
		}

		if (!config.containsKey(IProducer.PRODUCER_TOPIC)) {
			throw new IllegalArgumentException("Missing configuration "
					+ IProducer.PRODUCER_TOPIC);
		}
		String topic = config.getString(IProducer.PRODUCER_TOPIC);
		
		boolean autoCreate = config.getBoolean(IProducer.AUTO_CREATE, false);
		if (autoCreate) {
			if (!config.containsKey(IProducer.ZK_CONNECT)) {
				throw new IllegalArgumentException("Missing configuration "
						+ IProducer.ZK_CONNECT);
			}
			String zkServer = config.getString(IProducer.ZK_CONNECT);
			logger.info("EventBus zookeeper: " + zkServer);
			EventBusManager eventbusManager = new EventBusManager(zkServer);
			
			if (!eventbusManager.hasTopic(topic)) {
				if (!autoCreate)
					throw new IllegalStateException("Missing topic =" + topic);

				logger.info("auto creating topic =" + topic);
				boolean flag = eventbusManager.createTopic(topic,
						config.getInt(IProducer.TOPIC_REPLICATION_FACTOR, 1),
						config.getInt(IProducer.TOPIC_PARTITION_NUM, 3));
				if (!flag) {
					throw new IllegalStateException("fail to create topic - "
							+ topic);
				}
			}
		}
		logger.info("validation done");
	}


}
