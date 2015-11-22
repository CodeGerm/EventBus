package org.cg.eventbus.message;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.centrify.platform.data.GenericTask;

/**
 * 
 * @author liang.li
 *
 */
public class TenantRegistrator implements MessageHandler<GenericTask> {

	private static Logger LOG = Logger.getLogger(TenantRegistrator.class);
	@Override
	public void init(Configuration config) {
		// TODO Link to Database

	}

	@Override
	public boolean handle(GenericTask task) {
//		LOG.info(task);
		return true;
	}

}
