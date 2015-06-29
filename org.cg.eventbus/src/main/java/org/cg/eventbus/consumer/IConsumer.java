/**
 * 
 */
package org.cg.eventbus.consumer;

import org.cg.eventbus.IEventBus;

/**
 * @author yanlinwang
 *
 */
public interface IConsumer<K, V> extends IEventBus {
	
	// Constants for consumer
	public static final String DEFAULT_SUB_ZK_CONNECT = "localhost:2181";
	
	public static final String GROUP_ID = "group.id";
	public static final String DEFAULT_GROUP_ID = "test_group";
	
	//public static final String CLIENT_ID = "client.id";
	
	public static final String COMMIT_INTERVAL = "commit.interval";
	public static final String DEFAULT_COMMIT_INTERVAL = "60000";
	
	public static final String KEY_DECODER = "key.decoder";
	public static final String VALUE_DECODER = "value.decoder";

	
	public static final String CONSUMER_TOPICS = "topics";
	public static final String CONSUMER_STREAMS = "%s.streams";
	public static final String CONSUMER_LISTENERS = "%s.listeners";

	
	public static final String DEFAULT_STREAM_THREADS = "1";
	
	public static final String CONSUMER_THREAD_POOL_SIZE = "thread.pool.size";
	public static final String DEFAULT_THREAD_POOL_SIZE = "1000";
	
	// Constants for manager
	public static final String SINGLE_TOPIC = "single-topic";
	public static final String DEFAULT_SINGLE_TOPIC = "test";
	
	public static final int NO_SUCH_TOPIC = -1;
	public static final int NO_SUCH_BROKER = -1;
	
	// Constants for consumer
	public static final String ZKCONNECTION_TIMEOUT = "zk.connectiontimeout.ms";
	public static final String DEFAULT_CONNECTION_TIMEOUT = "100000";
	
	// Constants for client	
	public static final String DEFAULT_ZKCLIENT_SERVER = "localhost:2181";
	public static final int DEFAULT_ZKCLIENT_SESSION_TIMEOUT = 30000;
	public static final int DEFAULT_ZKCLIENT_CONNECTION_TIMEOUT = 30000;
}
