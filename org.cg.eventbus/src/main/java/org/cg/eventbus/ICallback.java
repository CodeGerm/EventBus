/**
 * 
 */
package org.cg.eventbus;

/**
 * @author yanlinwang
 *
 */
public interface ICallback {
	
	public class Response {
		long offset;
		int partition;
		String topic;
		public long getOffset() {
			return offset;
		}
		public void setOffset(long offset) {
			this.offset = offset;
		}
		public int getPartition() {
			return partition;
		}
		public void setPartition(int partition) {
			this.partition = partition;
		}
		public String getTopic() {
			return topic;
		}
		public void setTopic(String topic) {
			this.topic = topic;
		}
		@Override
		public String toString() {
			return "Response [offset=" + offset + ", partition=" + partition
					+ ", topic=" + topic + "]";
		}	

	}
	
	public void onCompletion(Response response, java.lang.Exception exception); 

}
