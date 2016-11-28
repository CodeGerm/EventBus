package org.cg.eventbus.stream;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.cg.eventbus.policy.IPolicy;

import kafka.message.MessageAndMetadata;

/**
 * 
 * @author liang.li
 *
 */
public class Dispatcher<K, V> extends Thread{
	
	private static final Logger LOG = Logger.getLogger(Dispatcher.class);
	
	private Configuration config;
	
	@SuppressWarnings("rawtypes")
	private Fetcher fetcher;
	
	private IHandler handler;

	private IReporter reporter;
	
	private IPolicy policy;
	
	
	public Dispatcher(Configuration config, IHandler handler, IReporter reporter, IPolicy policy) throws Exception {
		this.config = config;
		this.handler = handler;
		this.reporter = reporter;
		this.policy = policy;
		init();
	}

	@SuppressWarnings({ "rawtypes" })
	private void init() throws Exception {
		fetcher = new Fetcher(config);
		LOG.debug("Fetcher initialization finished.");
	}
	
	
	@SuppressWarnings("unchecked")
	public void run() {	
		while (fetcher.hasNext()) {
			MessageAndMetadata<K, V> data = fetcher.nextMessage();
			LOG.debug("Coming Data, key: " + data.key() + ", value: " + data.message());
			handler.handle(data, reporter);
			if (policy.pieceDone())
				fetcher.commit();
		}
	}
	
	public void close() {
		fetcher.close();
	}
	
	
}
