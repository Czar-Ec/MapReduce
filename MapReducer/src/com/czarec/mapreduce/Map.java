package com.czarec.mapreduce;

import java.util.ArrayList;

public class Map {

	/**
	 * list of key value pairs
	 * array list stores the key in one part and the value of the key in the other
	 * meaning the array list can store arrays of 2
	 */
	ArrayList<KeyValuePair1>keyValues = new ArrayList<KeyValuePair1>();
	
	
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
	public void put(Object key)
	{
		//new key value pair
		//all keys get given a value of 1
		KeyValuePair1 kv = new KeyValuePair1(key, 1);
		
		//add the key value to the key values list
		keyValues.add(kv);
	}
	
	public ArrayList<KeyValuePair1> get()
	{
		return keyValues;
	}
		
}
