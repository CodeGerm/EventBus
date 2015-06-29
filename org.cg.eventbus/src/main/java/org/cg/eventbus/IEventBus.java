/**
 * 
 */
package org.cg.eventbus;

/**
 * @author yanlinwang
 *
 */
public interface IEventBus {

	// configurations

	/**
	 * Kafka only uses ZK in consumer config, the reason we expose this to
	 * common configuration is we will optionally auto create the topic in
	 * server kick off
	 */
	public static final String ZK_CONNECT = "zookeeper.connect";
	
	public static final String AUTO_CREATE = "eventbus.autoCreate";
	
	

	// default configurations
	public static final String DEFAULT_PRO_ZK_CONNECT = "localhost:2181";

}
