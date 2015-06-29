/**
 * 
 */
package org.cg.eventbus;

/**
 * @author yanlinwang
 *
 */
public interface IEventListener<K, V> {

	boolean beforeProcess(K key, V msg);
	
	boolean process(K key, V msg);
	
	boolean postProcess(K key, V msg);

}