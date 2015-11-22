package org.cg.eventbus.message;

import org.apache.commons.configuration.Configuration;

/**
 * 
 * @author liang.li
 *
 */
public interface MessageHandler<T> {

	public void init(Configuration config);
	
	public boolean handle(T message);
}
