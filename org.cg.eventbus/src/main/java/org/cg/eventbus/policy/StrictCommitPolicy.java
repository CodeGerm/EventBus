package org.cg.eventbus.policy;

import org.cg.eventbus.message.Message;

/**
 * A very strict commit policy. <br>
 * Consumer will commit its offset after consume every single message.
 * @author liang.li
 *
 */
public class StrictCommitPolicy extends CommitPolicy{
	
	public StrictCommitPolicy() {
		super();
	}

	@Override
	public boolean needCommit() {
		return true;
	}

	@Override
	public boolean needCommit(Message msg) {
		return true;
	}

}
