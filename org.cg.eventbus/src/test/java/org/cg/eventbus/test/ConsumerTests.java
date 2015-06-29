/**
 * 
 */
package org.cg.eventbus.test;

import org.cg.eventbus.consumer.SymmetricDispatcher;

import junit.framework.TestCase;

/**
 * @author yanlinwang
 *
 */
public class ConsumerTests extends TestCase {

	public void testEventListener() throws Exception {
		SymmetricDispatcher<String, Event> dispatcher = new SymmetricDispatcher<String, Event>(
				"./example/consumer.prop");
		dispatcher.launch();
		System.in.read();
	}

}
