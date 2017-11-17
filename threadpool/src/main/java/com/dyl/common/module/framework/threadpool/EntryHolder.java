package com.dyl.common.module.framework.threadpool;

import java.util.concurrent.atomic.AtomicInteger;
/**
 * 
 * @author justdebugit@gmail.com
 *
 * @param <V>
 */
public class EntryHolder<V> implements IEntryHolder<V>{
	private volatile V value;
	private final AtomicInteger state;
	
	public EntryHolder(V v){
		this.value = v;
		state = new AtomicInteger();
	}
	
	public  EntryHolder(V v,int stateInt) {
		this.value = v;
		this.state = new AtomicInteger(stateInt);
	}

	public AtomicInteger state() {
		   return state;
	}

	public V get() {
		return value;
	}

	public void set(V v) {
		assert v !=null;
		value = v;
	}

	public String toString() {
		return "DefaultEntryHolder [value=" + value + ", state=" + state + "]";
	}
	

}
