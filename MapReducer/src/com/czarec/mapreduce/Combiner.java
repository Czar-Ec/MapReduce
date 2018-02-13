package com.czarec.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;

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
	ArrayList<KeyValuePair2> keyValues = new ArrayList<KeyValuePair2>();
	
	/**
	 * Combiner
	 * default constructor
	 */
	Combiner() {}
	
	/**
	 * Combiner
	 * constructor
	 * 
	 * @param kv
	 */
	Combiner(ArrayList<KeyValuePair1> kv)
	{
		//process the key value pairs into another set of key values
		for(int i = 0; i < kv.size(); i++)
		{
			//check if the key is already in the keyValues array list			
			if(keyValues.contains(kv.get(i).getKey()))
			{
				//add a value to its value array
				keyValues.get(i).addValue();
			}
			else
			{
				//make the second kv pair
				KeyValuePair2 kv2 = new KeyValuePair2(kv.get(i), new ArrayList<Integer>(Arrays.asList(1)));
				keyValues.add(kv2);
			}
			
			System.out.println(keyValues.get(i).toString());
		}
	}
}
