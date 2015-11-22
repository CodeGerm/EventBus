package org.cg.eventbus.parser;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.configuration.Configuration;
import org.cg.eventbus.message.Message;

import com.centrify.platform.data.GenericTask;

/**
 * 
 * @author liang.li
 *
 */
public class TaskMessageParser extends MessageParser<GenericTask>{

	private SpecificDatumReader<GenericTask> reader;
	
	public TaskMessageParser(Configuration config) {
		super(config);
		reader = new SpecificDatumReader<GenericTask>(GenericTask.class);
	}

	@Override
	public GenericTask parser(Message message) throws Exception {
		byte[] bytes = message.getPayload();
		BinaryDecoder binDecoder=DecoderFactory.get().binaryDecoder(bytes,null);
		GenericTask task = GenericTask.class.newInstance();
		reader.read(task, binDecoder);
		return task;
	}

}
