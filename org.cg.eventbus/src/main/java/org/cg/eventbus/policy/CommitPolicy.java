package org.cg.eventbus.policy;

import org.cg.eventbus.message.Message;

/**
 * 
 * @author liang.li
 *
 */
public abstract class CommitPolicy {

	public CommitPolicy(){
		
	}
	
	abstract public boolean needCommit();
	
	abstract public boolean needCommit(Message msg);
}
