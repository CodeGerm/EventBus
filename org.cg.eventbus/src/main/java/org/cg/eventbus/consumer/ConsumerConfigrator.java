/**
 * 
 */
package org.cg.eventbus.consumer;

import java.lang.reflect.Constructor;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.serializer.Decoder;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.cg.eventbus.ConfigUtil;
import org.cg.eventbus.EventBusManager;
import org.cg.eventbus.IEventBus;
import org.cg.eventbus.IEventListener;

/**
 * @author yanlinwang
 *
 */
public class ConsumerConfigrator {
	
	private static Logger logger = Logger.getLogger(ConsumerConfigrator.class); 

	
	/**
	 *   <p>mandatory properties are:
	 *   <ul>
	 *   	<li>group.id</li>
	 *   	<li>zookeeper.connect</li>
	 *   	<li>key.decoder </li>
	 *  	<li>value.decoder</li>
	 *  	<li>topics (a comma-separated String list)</li>
	 *   </ul>
	 *   </p>
	 * @param props incoming properites
	 * @throws ConfigurationException 
	 */
	public static void validate (Properties props) 
			throws ConfigurationException {
		String groupId;
		

		if (!props.containsKey(IConsumer.GROUP_ID)) {
			logger.error(IConsumer.GROUP_ID + " is missing");
			for (Object s : props.keySet()) {
				logger.error(s.toString() + ": " + props.get(s).toString());
			}
			throw new IllegalArgumentException(IConsumer.GROUP_ID + " is missing");
		}
		groupId = props.getProperty(IConsumer.GROUP_ID);
		
		logger.info("groupId: " + groupId);


		if (!props.containsKey(IEventBus.ZK_CONNECT)) {
			throw new IllegalArgumentException("missing zookeeper for groupId : " + groupId);
		}
		String client = props.getProperty(IConsumer.GROUP_ID)
				+ "_" + props.getProperty(IEventBus.ZK_CONNECT);

		logger.info( "validating event bus - " + client );
		
		
		if (!props.containsKey(IConsumer.KEY_DECODER)) {
			throw new IllegalArgumentException("missing key decoder config for event bus - " + client );
		}
		
		logger.info(client + " key_decoder: " + props.getProperty(IConsumer.KEY_DECODER));
	
		
		
		if (!props.containsKey(IConsumer.VALUE_DECODER)) {
			throw new IllegalArgumentException("missing value decoder config for event bus - " + client);
		}
		logger.info(client + " value_decoder: " + props.getProperty(IConsumer.VALUE_DECODER));
		
		
		if (!props.containsKey(IConsumer.CONSUMER_TOPICS)) {
			throw new IllegalArgumentException("missing topics config for event bus - " + client );
		}
		logger.info(client + " consumer topics: " + props.getProperty(IConsumer.CONSUMER_TOPICS));
	}

	
	public static Decoder configKeyDecoder(Properties props) {
		String decoderClassName = props.getProperty(IConsumer.KEY_DECODER);
		String groupId = props.getProperty(IConsumer.GROUP_ID);
			
			try {
				Class<?> keyClass = Class.forName(decoderClassName);
				Constructor<?> keyCons = keyClass.getConstructor(kafka.utils.VerifiableProperties.class);
				return (Decoder) keyCons.newInstance(new Object[] {null});
			} catch (Exception e) {
				logger.error("failed to initate decoder, ", e);
				throw new IllegalArgumentException("failed to initate key decoder groupId - " + groupId, e);
			}

		
	}
	
	@SuppressWarnings("rawtypes")
	public static Decoder configValueDecoder(Properties props) {

		String decoderClassName = props.getProperty(IConsumer.VALUE_DECODER);
		String groupId = props.getProperty(IConsumer.GROUP_ID);
			
			try {
				Class<?> valueClass = Class.forName(decoderClassName);
				Constructor<?> valueCons = valueClass.getConstructor(kafka.utils.VerifiableProperties.class);
				return (Decoder) valueCons.newInstance(new Object[] {null});
			} catch (Exception e) {
				logger.error("failed to initate decoder, ", e);
				throw new IllegalArgumentException("failed to initate value decoder groupId - " + groupId, e);
			}
	
	}
	
	public static Map<String, Integer> configTopicStreamsMap(Properties props) 
			throws Exception {
		
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		String groupId = props.getProperty(IConsumer.GROUP_ID);
		String zkServer = props.getProperty(IConsumer.ZK_CONNECT);
		String[] topicNames = props.getProperty(IConsumer.CONSUMER_TOPICS).split(",");
		
		EventBusManager manager = new EventBusManager(zkServer);
		for (String s : topicNames) {
			
			if (!manager.hasTopic(s)) {
				throw new IllegalStateException("failed to find configured topic - " + s + " groupId - " + groupId );
			}
			
			String threadQuery = String.format(IConsumer.CONSUMER_STREAMS, s);
			Integer numThreads = Integer.valueOf(props.getProperty(threadQuery, IConsumer.DEFAULT_STREAM_THREADS));
			if (0 >= numThreads.intValue()) {
				throw new IllegalArgumentException("failed to find configured thread number - " + numThreads + " groupId - " + groupId);
			}
			logger.info( " setup topic to thread map [" + s + ", " + numThreads + "]");
			retMap.put(s, numThreads);
		}
		
		return retMap;
	}
	

	@SuppressWarnings("rawtypes")
	public static Map<String, List<IEventListener>> configTopicListenersMap(Properties props) 
			throws Exception {
		
		Map<String, List<IEventListener>> retMap = new HashMap<String, List<IEventListener>>();
		String groupId = props.getProperty(IConsumer.GROUP_ID);
		String[] topicNames = props.getProperty(IConsumer.CONSUMER_TOPICS).split(",");
		
		for (String topic : topicNames) {
			String lsnrs = String.format(IConsumer.CONSUMER_LISTENERS, topic);
			
			if (!props.containsKey(lsnrs)) {
				throw new IllegalArgumentException("failed to locate topic properties - " + topic);
			}
			String[] listeners = props.getProperty(lsnrs).split(",");
			List<IEventListener> listenerList = new ArrayList<IEventListener>();
			
			for (String listener : listeners) {
				
				String listenerName = getLastSubstring(listener);
				String propQuery = String.format("%s.%s.", listeners, listenerName);
				logger.info("initializing listener " + listener + ", topic - " + topic );
				
				try {
					Properties listenerProp = ConfigUtil.extractProperties(props, propQuery);
					if (listenerProp.isEmpty()) {
						Class<?> listenClass = Class.forName(listener);
						Constructor<?> listenCons = listenClass.getConstructor();
						IEventListener listenValue = (IEventListener) listenCons.newInstance();
						listenerList.add(listenValue);
					}
					else {
						logger.info("with properties: " + listenerProp);
						Class<?> listenClass = Class.forName(listener);
						Constructor<?> listenCons = listenClass.getConstructor(java.util.Properties.class);
						IEventListener listenValue = (IEventListener) listenCons.newInstance(listenerProp);
						listenerList.add(listenValue);
					}
				} catch (Exception e) {
					throw new IllegalStateException("failed to initialize listener " + listener 
									+ "  topic - " + topic + ", group - " + groupId, e);
				}
			}
			
			retMap.put(topic, listenerList);
		}
		
		return retMap;
	}
	
	
	private static String getLastSubstring(String fullName) {
		StringBuilder sb = new StringBuilder(fullName);
		int point = sb.lastIndexOf(".");
		if (point <= 0) {
			throw new IllegalArgumentException("failed to located dot in " + fullName);
		}
		
		return sb.substring(point+1);
	}


}
