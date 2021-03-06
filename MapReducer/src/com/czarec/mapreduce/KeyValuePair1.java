package com.czarec.mapreduce;

public class KeyValuePair1 {
	
	//stores the key and the value of the key
	protected Object key;
	protected Object value;
	
	/**
	 * KeyValuePair1
	 * default constructor
	 */
	KeyValuePair1() {}
	
	/**
	 * KeyValuePair1
	 * constructor
	 * 
	 * @param key
	 * @param value
	 */
	KeyValuePair1(Object key, Object value)
	{
		this.key = key;
		this.value = value;
	}
	
	/**
	 * getKey
	 * returns the key from the key value pair
	 * 
	 * @return key
	 */
	public Object getKey() { return key; }
	
	/**
	 * getValue
	 * returns the value from the key value pair
	 * 
	 * @return value
	 */
	public Object getValue() { return value; }
	
	/**
	 * toString
	 * returns the keyvalue pair as a string
	 */
	public String toString()
	{
		return key + "\n" + value + "\n";
	}
}
