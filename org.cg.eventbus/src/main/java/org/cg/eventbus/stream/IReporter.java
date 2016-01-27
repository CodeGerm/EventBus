package org.cg.eventbus.stream;

/**
 * 
 * @author liang.li
 *
 */
public interface IReporter<V, T> {
	
	T preWork(V v);
	
	T report(V v);
	
	void close();

}
