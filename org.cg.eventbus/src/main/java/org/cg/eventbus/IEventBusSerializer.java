/**
 * 
 */
package org.cg.eventbus;

import kafka.serializer.Decoder;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Combination of serializer and deseriliazer for producer and consumer
 * 
 *  to use it in event bus, create a default constructor.
 * 
 * @author yanlinwang
 *
 */
public interface IEventBusSerializer<T> extends Serializer<T>, Deserializer<T>, Decoder<T>{

}
