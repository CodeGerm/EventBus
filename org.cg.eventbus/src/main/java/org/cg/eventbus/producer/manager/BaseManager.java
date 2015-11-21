package org.cg.eventbus.producer.manager;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.producer.AbstractEventProducer;

/**
 * 
 * @author liang.li
 *
 */
public abstract class BaseManager<V> {
	
	protected BaseManager() {}
	public static BaseManager getInstance(Configuration config) {
		return null;
	}
	
	abstract public AbstractEventProducer borrowProducer(String topic) throws Exception;
	
	abstract public void returnProducer(String topic, AbstractEventProducer producer);
	
	abstract public void send(V msg) throws Exception;
	
	/**
	 * Release resources manager and its producers/pools have.
	 */
	abstract public void close();
}
