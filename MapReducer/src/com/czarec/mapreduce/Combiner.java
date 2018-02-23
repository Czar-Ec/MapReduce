package com.czarec.mapreduce;

import java.io.File;
import java.util.ArrayList;

/**
 * class that combines the key value pairs into another
 * key value pair by combining the key's values into a list
 * @author User
 *
 */
public class Combiner {
	
	/*
	 * list of key value pairs (2)
	 * this has the second key value pairs where the values are a list of values
	 */
	ArrayList<Object> keyValues = new ArrayList<Object>();
	
	ArrayList<KeyValuePair2> kv2 = new ArrayList<KeyValuePair2>();
	
	/**
	 * Combiner
	 * constructor
	 */
	Combiner(Map map)
	{
		ArrayList<Object> kv = map.get();
		
		for(Object obj : kv)
		{
			//boolean to check if group exists
			boolean exists = false;
			
			//loop through the kv2
			for(KeyValuePair2 compare : kv2)
			{
				//if found
				if(compare.getKey().equals(((KeyValuePair1) obj).getKey()))
				{
					compare.addValue(((KeyValuePair1) obj).getValue());
				}
			}
			
			//if not found
			if(!exists)
			{
				//add new value
				ArrayList<Object> values = new ArrayList<Object>();
				values.add(((KeyValuePair1) obj).getValue());
				
				KeyValuePair2 newObj = new KeyValuePair2(((KeyValuePair1) obj).getKey(), values);
				kv2.add(newObj);
			}
		}
	}
	
	/**
	 * getKV2
	 * returns the combined kv1 values (kv2)
	 * 
	 * @return kv2
	 */
	public ArrayList<KeyValuePair2> getKV2()
	{
		return kv2;
	}
	
}


















