/**
 * 
 */
package org.cg.eventbus.test;

import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.cg.eventbus.IEventBusSerializer;

/**
 * @author yanlinwang
 *
 */
public class EventSerializer implements IEventBusSerializer<Event> {

	
	
	/* (non-Javadoc)
	 * @see kafka.serializer.Decoder#fromBytes(byte[])
	 */
	@Override
	public Event fromBytes(byte[] arg0) {
		// TODO Auto-generated method stub
		return (Event)SerializationUtils.deserialize(arg0);
	}

	@Override
	public Event deserialize(String arg0, byte[] arg1) {
		return (Event)SerializationUtils.deserialize(arg1);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, Event arg1) {
		return SerializationUtils.serialize(arg1);
	}

	
}
