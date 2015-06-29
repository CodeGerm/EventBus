/**
 * 
 */
package org.cg.eventbus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.cluster.Cluster;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.cg.eventbus.consumer.IConsumer;

import scala.collection.Seq;

/**
 * @author yanlinwang
 *
 */
public class EventBusManager {

	public class Connection {

		private String host;
		private int port;

		public Connection() {
			super();
		}

		public Connection(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public String toString() {
			return "Connection [host=" + host + ", port=" + port + "]";
		}

	}

	private Logger logger = Logger.getLogger(EventBusManager.class);
	private ZkClient zkClient;

	public EventBusManager(String zkServer) {
		kafka.utils.ZKStringSerializer$ mySerial = kafka.utils.ZKStringSerializer$.MODULE$;
		zkClient = new ZkClient(zkServer,
				IConsumer.DEFAULT_ZKCLIENT_SESSION_TIMEOUT,
				IConsumer.DEFAULT_ZKCLIENT_CONNECTION_TIMEOUT, mySerial);
	}

	public boolean isEventBusUp() {
		if (0 >= getEventBusSize())
			return false;
		return true;
	}

	public int getEventBusSize() {
		Cluster cluster = ZkUtils.getCluster(zkClient);
		return cluster.size();
	}

	public Map<Integer, Connection> getEventBusInfo() {
		Map<Integer, Connection> info = new HashMap<Integer, Connection>();
		int num = getEventBusSize();

		if (0 >= num) {
			logger.error("empty EventBus, size=" + num);
			return null;
		}

		Cluster cluster = ZkUtils.getCluster(zkClient);
		for (int i = 0; i < num; ++i) {
			Connection connect = new Connection(cluster.getBroker(i).get()
					.host(), cluster.getBroker(i).get().port());
			info.put(cluster.getBroker(i).get().id(), connect);
		}

		return info;
	}

	public boolean isBrokerUp(int id) {
		Map<Integer, Connection> info = getEventBusInfo();

		if (null == info) {
			logger.error("Failed to get EventBus info brokerId=" + id);
			return false;
		}

		if (info.containsKey(Integer.valueOf(id))) {
			return true;
		}

		logger.info("No broker in the EventBus brokerId=" + id);
		return false;
	}

	public List<String> getAllTopics() throws Exception {
		List<String> topicNames = null;

		try {
			Seq<String> topics = ZkUtils.getAllTopics(zkClient);
			topicNames = new ArrayList<String>();
			String[] topicStrings = new String[topics.size()];
			topics.copyToArray(topicStrings);
			topicNames = Arrays.asList(topicStrings);

		} catch (Exception e) {
			throw new Exception("Failed to get all the topics.", e);
		}

		return topicNames;
	}

	public boolean hasTopic(String topic) throws Exception {
		List<String> topics = getAllTopics();
		if (topics.contains(topic))
			return true;

		return false;
	}

	public int getPartitionByTopic(String topic) {
		Map<String, Integer> topicPartition = getAllPartitions();
		if (topicPartition.containsKey(topic))
			return topicPartition.get(topic);

		return IConsumer.NO_SUCH_TOPIC;
	}

	public boolean createTopic(String topic, int replicationFactor,
			int numPartitions) throws Exception {

		List<String> topicList = getAllTopics();
		if (null == topicList || topicList.contains(topic)) {
			logger.error("topic [" + topic + "] is existing in this EventBus");
			if (numPartitions <= getPartitionByTopic(topic))
				return false;
			else {
				logger.info("increase the partition number for " + topic
						+ "  from " + getPartitionByTopic(topic) + " to "
						+ numPartitions);
				increasePartitionByTopic(topic, numPartitions);
				return true;
			}
		}

		List<String> command = new ArrayList<String>();
		command.add("--create");
		command.add("--replication-factor");
		command.add(Integer.toString(replicationFactor));
		command.add("--partitions");
		command.add(Integer.toString(numPartitions));
		command.add("--topic");
		command.add(topic);

		String[] comd_string = new String[command.size()];
		command.toArray(comd_string);
		TopicCommandOptions opt = new TopicCommandOptions(comd_string);

		TopicCommand.createTopic(zkClient, opt);
		return true;
	}

	public boolean increasePartitionByTopic(String topic, int numPartitions) {

		Map<String, Integer> topicPartitions = getAllPartitions();
		if (!topicPartitions.containsKey(topic)) {
			logger.error("Failed to find topic [" + topic + "]");
			return false;
		}

		if (topicPartitions.get(topic).intValue() >= numPartitions) {
			logger.error("topic ["
					+ topic
					+ "] has "
					+ topicPartitions.get(topic).intValue()
					+ " partitions, which is more than partition size setting ["
					+ numPartitions + "]");
			return false;
		}

		List<String> command = new ArrayList<String>();
		command.add("--alter");
		command.add("--partitions");
		command.add(Integer.toString(numPartitions));
		command.add("--topic");
		command.add(topic);

		String[] comd_string = new String[command.size()];
		command.toArray(comd_string);
		TopicCommandOptions opt = new TopicCommandOptions(comd_string);

		TopicCommand.alterTopic(zkClient, opt);
		return true;

	}

	public Map<String, Integer> getAllPartitions() {

		Seq<String> topics = ZkUtils.getAllTopics(zkClient);
		scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> map = ZkUtils
				.getPartitionAssignmentForTopics(zkClient, topics);
		scala.collection.Iterator<String> it = map.keysIterator();

		Map<String, Integer> topicPartitions = new HashMap<String, Integer>();
		while (it.hasNext()) {
			String key = it.next();
			int numPartitions = map.get(key).get().size();
			topicPartitions.put(key, Integer.valueOf(numPartitions));
		}

		return topicPartitions;
	}

	public boolean deleteTopic(String topic) throws Exception {
		if (!hasTopic(topic)) {
			logger.error("This EventBus does not have this topic [" + topic
					+ "]");
			return false;
		}

		zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
		if (hasTopic(topic))
			return false;
		return true;
	}

	public static boolean pingServer(String zkServer) {
		EventBusManager manager = new EventBusManager(zkServer);
		return manager.isEventBusUp();
	}

	public void shutdown() {
		this.zkClient.close();
	}
}
