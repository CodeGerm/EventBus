package org.cg.eventbus.message;

import java.util.Arrays;

/**
 * 
 * @author liang.li
 *
 */
public class Message {

	private String mTopic;
	private long mOffset;
	private byte[] mKey;
	private byte[] mPayload;
	private int mPartition;

	public Message(String topic, long offset, byte[] key, byte[] payload, int partition) {
		super();
		this.mTopic = topic;
		this.mOffset = offset;
		this.mKey = key;
		this.mPayload = payload;
		this.mPartition = partition;
	}
	
	public Message(String topic, long offset, byte[] key, byte[] payload) {
		super();
		this.mTopic = topic;
		this.mOffset = offset;
		this.mKey = key;
		this.mPayload = payload;
	}

	public String getTopic() {
		return this.mTopic;
	}

	public long getOffset() {
		return this.mOffset;
	}

	public byte[] getKey() {
		return this.mKey;
	}

	public byte[] getPayload() {
		return this.mPayload;
	}

	public int getPartition() {
		return this.mPartition;
	}

	@Override
	public String toString() {
		return "Message [Topic=" + mTopic + ", Offset=" + mOffset + ", Key=" + Arrays.toString(mKey) + ", Payload="
				+ Arrays.toString(mPayload) + ", Partition=" + mPartition + "]";
	}
	
	
}
