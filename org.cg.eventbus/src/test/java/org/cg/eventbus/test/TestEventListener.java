/**
 * 
 */
package org.cg.eventbus.test;

import org.cg.eventbus.IEventListener;

/**
 * @author yanlinwang
 *
 */
public class TestEventListener implements IEventListener<String, Event>{

	/* (non-Javadoc)
	 * @see org.cg.eventbus.IEventListener#beforeProcess(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean beforeProcess(String key, Event msg) {
		// TODO Auto-generated method stub
		return true;
	}

	/* (non-Javadoc)
	 * @see org.cg.eventbus.IEventListener#process(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean process(String key, Event msg) {
		System.out.println(msg);
		return true;
	}

	/* (non-Javadoc)
	 * @see org.cg.eventbus.IEventListener#postProcess(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean postProcess(String key, Event msg) {
		// TODO Auto-generated method stub
		return true;
	}

}
