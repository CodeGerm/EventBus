/**
 * 
 */
package org.cg.eventbus;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.GetValue;

/**
 * @author yanlinwang, liang.li
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
	
	public static Configuration extractConfiguration(Configuration conf, String subKey) {
		
		Configuration config = new PropertiesConfiguration();
		Iterator<String> iter = conf.getKeys();
		while(iter.hasNext()) {
			String str = iter.next();
			if (str.contains(subKey)) {
				StringBuilder sb = new StringBuilder(str);
				int point = sb.lastIndexOf(subKey);
				String newKey = sb.substring(point + subKey.length());
				config.setProperty(newKey, conf.getProperty(str));
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
	
	public static Configuration getConfigurationFromConsul(String consulUrl, String prefix) {
		ConsulClient client = new ConsulClient(consulUrl);
		if (0 >= client.getKVValues(prefix).getValue().size())
			return null;
		Configuration config = new PropertiesConfiguration();
		
		List<GetValue> pairs = client.getKVValues(prefix).getValue();
		for (GetValue pair : pairs) {
			config.setProperty(pair.getKey().substring(prefix.length()), 
					new String(Base64.decodeBase64(pair.getValue())));
		}
		return config;
	}
}
