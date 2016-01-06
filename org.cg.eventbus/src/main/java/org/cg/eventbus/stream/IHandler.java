package org.cg.eventbus.stream;

import kafka.message.MessageAndMetadata;

public interface IHandler<K, V> {
	
	void init(IReporter<K, V> reporter);
	
	void handle(MessageAndMetadata<K, V> data);

}
