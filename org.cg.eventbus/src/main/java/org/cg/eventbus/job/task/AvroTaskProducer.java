package org.cg.eventbus.job.task;

import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.producer.AbstractEventProducer;

import com.centrify.platform.data.GenericTask;

public class AvroTaskProducer extends AbstractEventProducer<String, GenericTask>{

	public AvroTaskProducer(Configuration config) throws Throwable {
		super(config);
		// TODO Auto-generated constructor stub
	}
	
	public AvroTaskProducer(String cfgPath, String prefix) throws Throwable {
		super(cfgPath, prefix);
		// TODO Auto-generated constructor stub
	}
	
	public AvroTaskProducer(String cfgPath) throws Throwable {
		super(cfgPath);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getKey(GenericTask task) {
		// TODO Auto-generated method stub
		return String.valueOf(task.getInitSource().size());
	}

}
