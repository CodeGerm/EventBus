package org.cg.eventbus.hook;

import org.apache.log4j.Logger;

import kafka.javaapi.consumer.ConsumerConnector;

/**
 *  Hook for event bus Fetcher.
 * @author liang.li
 *
 */
public class FetcherHook extends Thread{
	
	private Logger LOG = Logger.getLogger(FetcherHook.class);
	
	private ConsumerConnector connector;

	public FetcherHook(ConsumerConnector connector) {
		this.connector = connector;
	}
	
	public void run() {
		connector.shutdown();
		LOG.info("Hook job is triggered. Kafka Consumer Connector is shutdown.");
	}
}
