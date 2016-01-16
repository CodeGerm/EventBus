package org.cg.eventbus.stream;

/**
 * 
 * @author liang.li
 *
 * @param <K> key of one record in kafka
 * @param <V> value of this record
 */
public interface IReporter<T> {
	
	void report(T t);
	
	void close();

}
