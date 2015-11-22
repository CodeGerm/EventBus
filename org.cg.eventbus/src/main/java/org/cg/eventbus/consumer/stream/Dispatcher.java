package org.cg.eventbus.consumer.stream;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.cg.eventbus.message.MessageHandler;
import org.cg.eventbus.stream.policy.CommitPolicy;


/**
 * 
 * @author liang.li
 *
 */
public abstract class Dispatcher extends Thread{
	
	protected static Logger LOG = Logger.getLogger(Dispatcher.class);

	protected Configuration config;
	protected Configuration handlerConfig;
	protected Configuration parserConfig;
	protected GenericFetcher fetcher;
	protected CommitPolicy policy;
	protected boolean commit;
	protected MessageHandler handler;
	
	public Dispatcher(Configuration conf, Configuration handlerConf, Configuration parserConf, CommitPolicy cmtPolicy){
		this.config = conf;
		this.handlerConfig = handlerConf;
		this.parserConfig = parserConf;
		this.policy = cmtPolicy;
	}
	
	private void init() {
		fetcher = new GenericFetcher(config);
		initHandler();
	}
	
	abstract public void initHandler();
	
	abstract public boolean poll();
	
	@Override
	public void run() {
		try {
            init();
        } catch (Exception e) {
        	LOG.error("consumer initialization failed. ");
        	e.printStackTrace();
            throw new RuntimeException("Failed to initialize the consumer" + e);
        }
		
		while (true) {
            boolean hasMoreMessages;
			hasMoreMessages = poll();
			if (!hasMoreMessages) {
                break;
            }
            
            if (commit)
            	fetcher.commit();
        }
	}
}
