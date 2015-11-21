package org.cg.eventbus.job.task;

import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.cg.eventbus.producer.AbstractEventProducer;
import org.cg.eventbus.producer.EventProducerPool;
import org.cg.eventbus.producer.manager.BaseManager;
import org.cg.eventbus.producer.manager.ManagerConfigurator;

import com.centrify.platform.data.GenericTask;
import com.centrify.platform.data.TaskProcessingCode;

/**
 * 
 * @author liang.li
 * @param <V>
 *
 */
public class TaskManager extends BaseManager<GenericTask>{

	private static Logger LOG = Logger.getLogger(TaskManager.class);
	private static volatile TaskManager instance = null;
	private static final int PARTITION_NUM = 1;
	
	private static EventProducerPool taskPool;
	private static EventProducerPool retryTaskPool;
	
	private TaskManager(){
		super();
	}
	public static TaskManager getInstance(Configuration config) {
		if (null == instance) {
			synchronized(TaskManager.class) {
				instance = new TaskManager();
				try {
					init(config);
				} catch (Exception e) {
					LOG.error("Initialize TaskManager failed");
					LOG.error(e);
				}
			}
		}
		return instance;
	}

	private static void init(Configuration config) throws Exception {
		Map<String, Configuration> topicConf = ManagerConfigurator.getConfig(config);
		
		String[] topics = new String[topicConf.size()];
		topicConf.keySet().toArray(topics);
		if (1 != topics.length) {
			LOG.error("Configuration file contains more than ONE topic.");
			throw new IllegalArgumentException("Configuration file contains more than ONE topic: " + topicConf.keySet());
		}
		
		String topic = topics[0];
		taskPool = new EventProducerPool<String, GenericTask> (
				AvroTaskProducer.class, topicConf.get(topic));
		
		String failedTopic = topic + ".retry";
		Configuration retryConf = ManagerConfigurator.cloneTaskConfig(topicConf.get(topic), failedTopic, PARTITION_NUM);
		retryTaskPool = new EventProducerPool<String, GenericTask>(
				AvroTaskProducer.class, retryConf);
	}

	@Override
	public void close() {
		taskPool.close();
		retryTaskPool.close();
	}
	
	@Override
	public AbstractEventProducer borrowProducer(String topic) throws Exception {
		switch(topic) {
			case "task": return (AbstractEventProducer)taskPool.borrowObject();
			case "retry": return (AbstractEventProducer)retryTaskPool.borrowObject();
			default: return null;
		}
	}
	
	@Override
	public void returnProducer(String topic, AbstractEventProducer producer) {
		switch(topic) {
			case "task": taskPool.returnObject(producer);break;
			case "retry": retryTaskPool.returnObject(producer);break;
			default: throw new RuntimeException("return producer [" + topic + "] failed");
		}
	}
	
	@Override
	public void send(GenericTask msg) throws Exception {
		if (msg.getStatus().equals(TaskProcessingCode.RETRY)) {
			AvroTaskProducer producer = (AvroTaskProducer)retryTaskPool.borrowObject();
			producer.send(msg, null);
			retryTaskPool.returnObject(producer);
		} else {
			AvroTaskProducer producer = (AvroTaskProducer)taskPool.borrowObject();
			producer.send(msg, null);
			taskPool.returnObject(producer);
		}
	}
}
