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
	ArrayList<Object> keyValues = new ArrayList<Object>();
	
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
		keyValues.addAll(kv);
	}
	
	public void printList()
	{
		for(int i = 0; i < keyValues.size(); i++)
		{
			//System.out.println(i + " " + keyValues.get(i).toString());
		}
	}
	
	public void add(ArrayList<Object> kv)
	{
		keyValues.addAll(kv);
	}
}
