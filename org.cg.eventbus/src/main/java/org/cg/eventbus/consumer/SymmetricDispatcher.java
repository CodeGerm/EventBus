/**
 * 
 */
package org.cg.eventbus.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.cg.eventbus.IEventListener;

/**
 * SymmetricDispatcher has one kafka consumer, dispatcher can be launched into
 * multiple processes, kafka should auto balance its consumers therefore it is
 * symmetric. One dispatcher/consumer can take msgs from multiple topic and each
 * topic get multiple streams for multiple thread to work on. (One partition
 * will only end up with one stream, one stream has msgs from 0 to n
 * partitions).
 * 
 * @author yanlinwang
 *
 */
public class SymmetricDispatcher<K, V> implements IConsumer<K, V> {

	private static final Logger logger = Logger.getLogger(SymmetricDispatcher.class);

	private String groupID;

	// private long commitInterval;

	private Properties config;

	private ConsumerConnector consumer;

	private Decoder<K> keyDecoder;
	private Decoder<V> valueDecoder;

	private List<String> topics;

	private Map<String, Integer> topic_streamNumMap;

	private Map<String, List<IEventListener>> topic_listenersMap;

	private ExecutorService executor;

	public SymmetricDispatcher(String configFile) throws Exception {
		this(new PropertiesConfiguration(configFile));
	}

	public SymmetricDispatcher(Configuration config) throws Exception {

		this(ConfigurationConverter.getProperties(config));
	}

	public SymmetricDispatcher(Properties props) throws Exception {
		config = props;
		init();
		setup();
	}

	private void init() throws Exception {

		ConsumerConfigrator.validate(config);

		groupID = this.config.getProperty(IConsumer.GROUP_ID) + "_"
				+ this.config.getProperty(IConsumer.CONSUMER_TOPICS);
		logger.info("consumer [" + groupID + "] has following properties: "
				+ config);

		// Create consumer according to input properties
		ConsumerConfig cfg = new ConsumerConfig(config);
		this.consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(cfg);

		logger.info("consumer [" + groupID + "] has following properties: "
				+ config);

		logger.info("Start consumer - " + Thread.currentThread().getName()
				+ ", group - " + groupID);

		// Set client name and commit time interval for this consumer
		// setCommitInterval();
	}

	/**
	 * initiate start up settings
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void setup() throws Exception {

		this.keyDecoder = ConsumerConfigrator.configKeyDecoder(this.config);
		this.valueDecoder = ConsumerConfigrator.configValueDecoder(this.config);

		topic_streamNumMap = ConsumerConfigrator
				.configTopicStreamsMap(this.config);
		logger.info("consumer [" + groupID + "] has thread Map: "
				+ topic_streamNumMap);

		this.topic_listenersMap = ConsumerConfigrator
				.configTopicListenersMap(this.config);
		logger.info("consumer [" + groupID + "] has listener Map: "
				+ topic_listenersMap);

		this.topics = new ArrayList<String>(topic_streamNumMap.keySet());
	}

	/**
	 * Just get client ID
	 * 
	 * @return client ID
	 */
	public String getGroupId() {
		return groupID;
	}

	// public void setCommitInterval() {
	// this.commitInterval =
	// Long.valueOf(this.config.getProperty(ConsumerConstants.COMMIT_INTERVAL,
	// ConsumerConstants.DEFAULT_COMMIT_INTERVAL)).longValue();
	// }
	//
	// public long getCommitInterval() {
	// return commitInterval;
	// }

	/**
	 * launch processor thread N topic, each topic -> M stream -> M thread </p>
	 */
	@SuppressWarnings("rawtypes")
	public void launch() {

		// Get stream for this consumer from queue
		Map<String, List<KafkaStream<K, V>>> topicStramMap = consumer
				.createMessageStreams(topic_streamNumMap, keyDecoder,
						valueDecoder);

		// Build thread executor for thread creation
		int threadsPoolSize = Integer.parseInt(this.config.getProperty(
				IConsumer.CONSUMER_THREAD_POOL_SIZE,
				IConsumer.DEFAULT_THREAD_POOL_SIZE));
		this.executor = Executors.newFixedThreadPool(threadsPoolSize);
		logger.info("consumer [" + groupID + "] has thread pool with size "
				+ threadsPoolSize);

		//
		for (String topic : this.topics) {
			List<KafkaStream<K, V>> streams = topicStramMap.get(topic);
			for (KafkaStream stream : streams) {
				List<IEventListener> list = this.topic_listenersMap.get(topic);
				String threadName = "EventBus dispatcher group - " + groupID
						+ " topic -" + topic;
				logger.info(threadName + " is launching, listener# = "
						+ list.size());
				executor.submit(new EventPuller(stream, threadName,
						this.topic_listenersMap.get(topic)));
			}
		}
	}

	/**
	 * Shutdown this CONSUMER, set offsets correctly and then shutdown
	 * everything
	 */
	public void shutdown() {
		logger.info("consumer [" + groupID + "] Shutting down....");
		consumer.commitOffsets();
		consumer.shutdown();
		executor.shutdown();
	}

	private class EventPuller implements Runnable {

		@SuppressWarnings("rawtypes")
		private KafkaStream stream;
		private String name;
		@SuppressWarnings("rawtypes")
		private List<IEventListener> listeners;

		/**
		 * 
		 * @param i_stream
		 *            Incoming stream, related to a topic
		 * @param i_threadNumber
		 *            Thread sequence ID in this consumer
		 * @param i_listeners
		 *            Listeners for this topic
		 */
		@SuppressWarnings("rawtypes")
		public EventPuller(KafkaStream i_stream, String threadName,
				List<IEventListener> i_listeners) {
			this.stream = i_stream;
			this.name = threadName;
			this.listeners = i_listeners;
			logger.info("consumer [" + groupID + "] is launching thread..");
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void run() {
			ConsumerIterator<K, V> it = stream.iterator();
			logger.info("In runnable thread[" + this.name + "]...");
			while (it.hasNext()) {
				MessageAndMetadata<K, V> data = it.next();
				K key = null;
				V message = null;
				try {
					key = data.key();
					message = data.message();
					// logger.info(this.name + " got key : " + key);
					// logger.info(this.name + " got message: " + message);
					for (IEventListener listener : this.listeners) {
						try {
							if (!listener.beforeProcess(key, message)) {
								logger.error("Job stopped at preHandle");
								continue;
							}
						} catch (Throwable e) {
							logger.error("Listener fail to pre handle event", e);
							continue;
						}

						try {
							if (!listener.process(key, message)) {
								logger.error("Job stopped at handleEvent");
								continue;
							}
						} catch (Throwable e) {
							logger.error("Listener fail to handle event", e);
							continue;
						}

						try {
							if (!listener.postProcess(key, message)) {
								logger.error("Job stopped at postHandle");
							}
						} catch (Throwable e) {
							logger.error("Listener fail to post handle event",
									e);
							continue;
						}
					}
				} catch (Throwable t) {
					logger.error("Fail to get message from stream: ", t);
					continue;
				}

			}
		}
	}

}
