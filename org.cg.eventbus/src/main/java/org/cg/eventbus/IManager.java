package org.cg.eventbus;

public interface IManager<K, V> extends IProducer<K, V>{

	public static final String TOPIC_LIST = "topics";
}
