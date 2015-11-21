package org.cg.eventbus.producer.manager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.cg.eventbus.ConfigUtil;
import org.cg.eventbus.IManager;
import org.cg.eventbus.IProducer;
import org.cg.eventbus.producer.ProducerConfigurator;

/**
 * 
 * @author liang.li
 *
 */
public class ManagerConfigurator {
	
	private static Logger LOG = Logger.getLogger(ManagerConfigurator.class);
	
	public static void validate(Configuration config) throws Exception {
		
		if (!config.containsKey(IManager.TOPIC_LIST)) {
			LOG.error("Missing configuration " + IManager.TOPIC_LIST);
			throw new IllegalArgumentException("Missing configuration " + 
						IManager.TOPIC_LIST);
		}
		
		String[] topics = config.getStringArray(IManager.TOPIC_LIST);
		for (String topicName : topics) {
			String prefix = topicName + ".";
			Configuration cfg = ConfigUtil.extractConfiguration(config, prefix);
			ProducerConfigurator.validate(cfg);
		}
	}
	
	public static Map<String, Configuration> getConfig(Configuration config) throws Exception {
		
		if (!config.containsKey(IManager.TOPIC_LIST)) {
			LOG.error("Missing configuration " + IManager.TOPIC_LIST);
			throw new IllegalArgumentException("Missing configuration " + 
						IManager.TOPIC_LIST);
		}
		
		Map<String, Configuration> topicConf = new HashMap<String, Configuration>();
		String[] topics = config.getStringArray(IManager.TOPIC_LIST);
		for (String topicName : topics) {
			String prefix = topicName + ".";
			Configuration cfg = ConfigUtil.extractConfiguration(config, prefix);
			ProducerConfigurator.validate(cfg);
			topicConf.put(topicName, cfg);
		}
		
		return topicConf;
	}
	
	public static Configuration cloneTaskConfig(Configuration config, String topicName, int partitionNum) {
		
		Configuration conf = new PropertiesConfiguration();
		Iterator<String> iter = config.getKeys();
		while (iter.hasNext()) {
			String key = iter.next();
			if (key.equalsIgnoreCase(IProducer.PRODUCER_TOPIC))
				conf.addProperty(key, topicName);
			else if (key.equalsIgnoreCase(IProducer.TOPIC_PARTITION_NUM))
				conf.addProperty(key, partitionNum);
			else
				conf.addProperty(key, config.getProperty(key));
		}
		return conf;
	}

}
