package org.cg.eventbus.job.task;

import org.cg.eventbus.AvroSerializer;

import com.centrify.platform.data.GenericTask;

/**
 * 
 * @author liang.li
 *
 */
public class GenericTaskSerializer extends AvroSerializer<GenericTask>{

	public GenericTaskSerializer() {
		super(GenericTask.class);
	}

}
