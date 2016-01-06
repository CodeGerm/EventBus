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
	
	private Logger LOG = Logger.getLogger(Dispatcher.class);
	
	private Configuration config;
	
	@SuppressWarnings("rawtypes")
	private Fetcher fetcher;
	@SuppressWarnings("rawtypes")
	private IHandler handler;
	@SuppressWarnings("rawtypes")
	private IReporter reporter;
	private IPolicy policy;
	
	
	public Dispatcher(Configuration config) throws Exception {
		this.config = config;
		init();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void init() throws Exception {
		fetcher = new Fetcher(config);
		LOG.debug("Fetcher initialization finished.");
		
		// Initialize policy
		String policyName = config.getString("policy.class");
		Class<IPolicy> policyClass = (Class<IPolicy>) Class.forName(policyName);
		policy = policyClass.newInstance();
		policy.init();
		LOG.debug("Policy initialization finished.");
		
		// Initialize reporter
		String reporterName = config.getString("reporter.class");
		Class<IReporter> reporterClass = (Class<IReporter>) Class.forName(reporterName);
		reporter = reporterClass.newInstance();
		reporter.init();
		LOG.debug("Reporter initialization finished.");
		
		// Initialize handler
		String handlerName = config.getString("handler.class");
		Class<IHandler> handlerClass = (Class<IHandler>) Class.forName(handlerName);
		handler = handlerClass.newInstance();
		handler.init();
		LOG.debug("Handler initialization finished.");
	}
	
	
	@SuppressWarnings("unchecked")
	public void run() {	
		while (fetcher.hasNext()) {
			MessageAndMetadata<K, V> data = fetcher.nextMessage();
			handler.handle(data, reporter);
			if (policy.pieceDone())
				fetcher.commit();
		}
	}
	
	public void close() {
		fetcher.close();
	}
	
	
}
