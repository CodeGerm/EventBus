package centrify.test.kafka;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.cg.eventbus.job.task.TaskManager;
import org.cg.eventbus.producer.AbstractEventProducer;

import com.centrify.platform.data.GenericTask;
import com.centrify.platform.data.TaskProcessingCode;
import com.centrify.platform.data.TaskTypeCode;

public class TaskManagerTest {
	
	private Logger LOG = Logger.getLogger(TaskManagerTest.class);
	private ExecutorService executor;
	private Configuration config;
	private int threadNum;
	
	public static void main(String[] args) throws Exception {
		TaskManagerTest myTest = new TaskManagerTest();
		myTest.init();
		myTest.run(50);
	}
	
	public void init() throws Exception {
		Configuration config = new PropertiesConfiguration("./config/producerManager.conf");
		this.config = config;
		threadNum = 10;
		this.executor = Executors.newFixedThreadPool(threadNum);
	}

	public void run(int num) {
		
        for (int i = 0; i < threadNum; ++i) {
        	Sender sender = new Sender(config, getTopic(i), num);
        	executor.submit(sender);
        }
	}
	
	public GenericTask getGenericTask(TaskProcessingCode code) {
		GenericTask task = new GenericTask();
		task.setType(TaskTypeCode.TENANT_REG);
		Map<CharSequence, CharSequence> tmpMap = new HashMap<CharSequence, CharSequence>();
		long num = Math.round(Math.random() * 1000) % 10L;
		for (int i=0; i<num; ++i) {
			tmpMap.put(String.valueOf(i), UUID.randomUUID().toString());
		}
		task.setInitSource(tmpMap);
		task.setCreationTime(System.currentTimeMillis());
		task.setRetryNum(0);
		task.setStatus(code);
		System.out.println(task);
		return task;
	}
	
	private class Sender implements Runnable {

		private TaskManager manager;
		private String topic;
		private int num;
		
		public Sender(Configuration config, String topic, int num) {
			manager = TaskManager.getInstance(config);
			this.topic = topic;
			this.num = num;
		}
		@Override
		public void run() {
			for (int i=0; i<num; ++i) {
				AbstractEventProducer<String, GenericTask> producer = null;
				try {
					TaskProcessingCode code = getCode(i);
					GenericTask task = getGenericTask(code);
					String topic = code == TaskProcessingCode.RETRY ? "retry" : "task";
					producer = manager.borrowProducer(topic);
					producer.send(task, null);
					manager.returnProducer(topic, producer);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			for (int i=0; i<num; ++i) {
				TaskProcessingCode code = getCode(i);
				GenericTask task = getGenericTask(code);
				try {
					manager.send(task);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public String getTopic(int i) {
		switch (i % 3) {
		case 0: return "highTask";
		case 1: return "lowTask";
		case 2: return "deadTask";
		default:
			return "";
		}
	}
	
	public TaskProcessingCode getCode(int i) {
		if (i % 10 > 1)
			return TaskProcessingCode.PENDING;
		else
			return TaskProcessingCode.RETRY;
	}
}
