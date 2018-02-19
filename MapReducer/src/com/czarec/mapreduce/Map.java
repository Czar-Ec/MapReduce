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
		if(keyValues.size() > 0)
		{
			//if the value doesn't exist in the array, add it as a kv1
			if(!keyValues.contains(new KeyValuePair1(key, value)))
			{
				
			}
		}
		else
		{
			//add the first value to the list
			KeyValuePair1 kv = new KeyValuePair1(key, value);
			keyValues.add(kv);
		}
	}
	
	public ArrayList<Object> get()
	{
		return keyValues;
	}
		
}
