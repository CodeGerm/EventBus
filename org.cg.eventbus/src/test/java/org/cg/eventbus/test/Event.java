package org.cg.eventbus.test;

import java.io.Serializable;

public class Event implements Serializable{
	
	public Event(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}
	String key;
	String value;
	@Override
	public String toString() {
		return "Event [key=" + key + ", value=" + value + "]";
	}
	
}
