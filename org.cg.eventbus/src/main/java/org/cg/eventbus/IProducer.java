/**
 * 
 */
package org.cg.eventbus;

import java.util.List;

/**
 * interface to further simplify the pub sub api 
 * 
 * @author yanlinwang, liang.li
 *
 */



public interface IProducer<K,V> extends IEventBus{
	
	public static final String BROKER_LIST = "bootstrap.servers";
	public static final String PRODUCER_TOPIC = "producer.topic.name";	
	public static final String PRO_TYPE = "producer.type";	
	public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
	
	public static final String KEY_SERIALIZER_CLASS = "key.serializer";
	public static final String VALUE_SERIALIZER_CLASS = "value.serializer";
	
	public static final String BYTE_SERIALIZER_CLASS = "kafka.serializer.DefaultEncoder";
	public static final String STRING_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";	
	public static final String PARTITION_CLASS = "partitioner.class";
	
	public static final String DEFAULT_KEY = "default";
	public static final String TOPIC_PARTITION_NUM = "producer.topic.numPartitions";
	public static final String TOPIC_REPLICATION_FACTOR = "producer.topic.replicationFactor";
	
	public static final String RECOVERY_FILE_PATH = "producer.recovery.file.path";

	
	K getKey(V msg);

	void send(V msg, ICallback callback);	
	void send(List<V> msgs,  ICallback callback );
	
	void send(K key, V msg, ICallback callback);
	void send(List<K> keys, List<V> msgs, ICallback callback);
	
	void close();	
  
}
