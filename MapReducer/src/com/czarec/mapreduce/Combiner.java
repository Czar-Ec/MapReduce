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
	Combiner(ArrayList<Object> kv)
	{
		for(int i = 0; i < kv.size(); i++)
		{
			//System.out.println(kv.get(i).toString());
		}
	}
}
