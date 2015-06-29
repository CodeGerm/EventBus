/**
 * 
 */
package org.cg.eventbus;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author yanlinwang
 *
 */
public class ConfigUtil {

	/**
	 * extract sub domain configurations
	 */
	public static Configuration extractConfiguration(Properties props,
			String subKey) {

		Configuration config = new PropertiesConfiguration();
		for (Object key : props.keySet()) {
			String str = (String) key;
			if (str.contains(subKey)) {
				StringBuilder sb = new StringBuilder(str);
				int point = sb.lastIndexOf(subKey);
				String newKey = sb.substring(point + subKey.length());
				config.setProperty(newKey, props.get(key));
			}
		}
		return config;
	}

	public static Properties extractProperties(Properties props, String subKey) {
		
		Properties retProp = new Properties();
		for (Object key : props.keySet()) {
			String str = (String) key;
			if (str.contains(subKey)) {
				StringBuilder sb = new StringBuilder(str);
				int point = sb.lastIndexOf(subKey);
				String newKey = sb.substring(point+subKey.length());
				retProp.put(newKey, props.get(key));
			}
		}
		return retProp;
	}
}
