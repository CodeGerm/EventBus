/**
 * 
 */
package org.cg.eventbus.producer;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;

/**
 * @author yanlinwang
 *
 */
public class PooledEventProducerFactory <K, V> extends BasePooledObjectFactory<AbstractEventProducer<K, V>> {
	
	private Logger logger = Logger.getLogger(PooledEventProducerFactory.class);
	
	private Configuration producerConf;
	private AtomicInteger count = new AtomicInteger(0);
	private Class<AbstractEventProducer> poolType;
	private Constructor pooledConstructor;
		
	public PooledEventProducerFactory (Class<AbstractEventProducer> poolType, String fileName) 
			throws ConfigurationException {
		this(poolType,new PropertiesConfiguration(fileName));
	}
	
	public PooledEventProducerFactory (Class<AbstractEventProducer> poolType, Configuration config) {
		this.poolType = poolType;
		producerConf = config;
		try {
			pooledConstructor = poolType.getConstructor(Configuration.class);
		} catch (Exception e) {
			logger.error("failed to get pooled object constructor", e);
		} 
		
	}
	
	@Override
	public AbstractEventProducer<K, V> create() throws Exception {
		
		AbstractEventProducer<K, V> producer = ( AbstractEventProducer<K, V> ) pooledConstructor.newInstance(new Object[]{producerConf});
		logger.info("Number of producer created " + count.addAndGet(1));
		return producer;
	}

	@Override
	public PooledObject<AbstractEventProducer<K, V>> wrap(
			AbstractEventProducer<K, V> provider) {
		return new DefaultPooledObject<AbstractEventProducer<K,V>>(provider);
	}

}
