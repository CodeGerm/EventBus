/**
 * 
 */
package org.cg.eventbus.test;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.producer.AbstractEventProducer;

/**
 * @author yanlinwang
 *
 */
public class ProducerTests extends TestCase {
	
	
	public class TestProducer extends AbstractEventProducer<String, Event>{
		
		public TestProducer(String cfgPath, String prefix) throws Exception {
			super(cfgPath, prefix);
			// TODO Auto-generated constructor stub
		}

		public TestProducer(Configuration config) throws Exception {
			super(config);
		}

		public TestProducer(String cfgPath) throws Exception {
			super(cfgPath);
		}

		@Override
		public String getKey(Event msg) {
			return msg.key;
		}
		
	}
	
		
	
	public void testProducer() throws Exception{
		TestProducer tp = new TestProducer("./example/producer.prop", "flow.sources.source.kafkaChannel.");
		for (int i =0 ; i<100 ; i++){			
			tp.send(new Event(String.valueOf(i), "message " + String.valueOf(i) ), null);
		}
	}

}
