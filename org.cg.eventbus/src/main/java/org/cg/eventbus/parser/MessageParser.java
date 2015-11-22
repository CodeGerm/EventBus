package org.cg.eventbus.parser;

import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.message.Message;

/**
 * 
 * @author liang.li
 *
 * @param <V>
 */
public abstract class MessageParser<V> {
	
	public MessageParser(Configuration config) {
		
	}
	
	abstract public V parser(Message msg) throws Exception;

}
