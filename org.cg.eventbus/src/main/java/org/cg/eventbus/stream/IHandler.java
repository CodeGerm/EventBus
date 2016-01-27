package org.cg.eventbus.stream;

import kafka.message.MessageAndMetadata;

public interface IHandler<K, V> {

	void handle(MessageAndMetadata<K, V> data, IReporter reporter);

}
