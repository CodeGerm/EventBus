/**
 * 
 */
package org.cg.eventbus.producer;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * @author yanlinwang
 *
 */
public class EventProducerPool<K, V> extends GenericObjectPool<AbstractEventProducer<K, V>>{

	public static final String POOL_SIZE = "pool.size";
	public static final String IDLE_SIZE = "idle.size";

	
	public EventProducerPool (Class<? extends AbstractEventProducer> poolType, Configuration conf) throws Exception{		
		super(new PooledEventProducerFactory<K, V>(poolType, conf));
		this.setMaxIdle(conf.getInt(IDLE_SIZE,GenericKeyedObjectPoolConfig.DEFAULT_MAX_IDLE_PER_KEY));
		this.setMaxTotal(conf.getInt(POOL_SIZE,GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
		this.setTestOnBorrow(true);
		this.setTestOnReturn(true);		
	}
}