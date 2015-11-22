package org.cg.eventbus.consumer.stream;

import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.message.Message;
import org.cg.eventbus.message.TenantRegistrator;
import org.cg.eventbus.parser.TaskMessageParser;
import org.cg.eventbus.stream.policy.CommitPolicy;
import org.cg.eventbus.stream.policy.StrictCommitPolicy;

import com.centrify.platform.data.GenericTask;

/**
 * 
 * @author liang.li
 *
 */
public class TaskDispatcher extends Dispatcher {

	private TaskMessageParser parser;
	
	public TaskDispatcher(Configuration conf, Configuration handlerConf, Configuration parserConf, CommitPolicy cmtPolicy) {
		super(conf, handlerConf, parserConf, cmtPolicy);
		this.policy = new StrictCommitPolicy();
		this.parser = new TaskMessageParser(null);
	}
	
	@Override
	public void initHandler() {
		this.handler = new TenantRegistrator();
		handler.init(handlerConfig);
	}

	@Override
	public boolean poll() {
		
		Message message = null;
		
		// TODO timeout
		boolean hasNext = fetcher.hasNext();
		if (!hasNext)
			return false;
		message = fetcher.read();
		
		if (null != message) {
			GenericTask task = null;
			try {
				task = parser.parser(message);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if (null != task) {
				handler.handle(task);
				commit = policy.needCommit();
			}
			
			//TODO
		}
		
		return true;
	}
}
