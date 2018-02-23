package com.czarec.mapreduce;

import java.util.ArrayList;

public class Map {

	/**
	 * list of key value pairs
	 * array list stores the key in one part and the value of the key in the other
	 * meaning the array list can store arrays of 2
	 */
	ArrayList<Object>keyValues = new ArrayList<Object>();
	
	
	/**
	 * Default map constructor
	 */
	Map()
	{
		
	}
	
	/**
	 * put
	 * adds a value to the map list
	 */
	public void put(Object key, Object value)
	{
		//check if the array is empty
		KeyValuePair1 kv = new KeyValuePair1(key, value);
		keyValues.add(kv);
	}
	
	/**
	 * get
	 * returns the key value pairs stored by the map
	 * 
	 * @return keyValues
	 */
	public ArrayList<Object> get()
	{
		return keyValues;
	}
}
