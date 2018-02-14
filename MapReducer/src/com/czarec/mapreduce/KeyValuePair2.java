package com.czarec.mapreduce;

import java.util.ArrayList;

/**
 * class inherits from KVPair1
 * uses same value types but the values are in a list
 * 
 * @author User
 *
 */
public class KeyValuePair2 extends KeyValuePair1 {
	
	//list of values
	private ArrayList<Object> valueList = new ArrayList<Object>();
	
	/**
	 * KeyValuePair2
	 * default constructor
	 */
	KeyValuePair2() {}
	
	/**
	 * KeyValuePair2
	 * constructor
	 * 
	 * @param key
	 * @param values
	 */
	KeyValuePair2(Object key, ArrayList<Object> values)
	{
		this.key = key;
		valueList = values;
	}
	
	/**
	 * addValue
	 * adds a value to the kvPair's value list
	 */
	public void addValue()
	{
		//add one more value of 1
		valueList.add(1);
	}
	
	public ArrayList<Object> getValues()
	{
		return valueList;
	}
	
	public String toString()
	{
		return "Key:\n" + key + "\n" +
				"Value: " + valueList.size() + "\n";
	}
}
