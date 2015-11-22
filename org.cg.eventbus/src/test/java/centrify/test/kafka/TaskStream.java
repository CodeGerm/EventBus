package centrify.test.kafka;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.cg.eventbus.consumer.stream.TaskDispatcher;

/**
 * 
 * @author liang.li
 *
 */
public class TaskStream {

	public static void main(String[] args) {
		
		int num = 1;
		List<TaskDispatcher> consumers = new LinkedList<TaskDispatcher>();
		
		for (int i=0; i<num; ++i) {
			TaskDispatcher dispatcher = new TaskDispatcher(getConfig(), null, null, null);
			consumers.add(dispatcher);
			dispatcher.start();
		}
		
		try{
			for (TaskDispatcher disp : consumers) {
				disp.join();
			}
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Configuration getConfig() {
		Configuration config = null;
		try {
			config = new PropertiesConfiguration("./config/stream.conf");
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
		return config;
	}
}
