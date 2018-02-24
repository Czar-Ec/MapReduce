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
	public void addValue(Object o)
	{
		//add one more value
		valueList.add(o);
	}
	
	/**
	 * addList
	 * appends a list to the current list
	 * @param obj
	 */
	public void addList(ArrayList<Object> obj)
	{
		for(Object o : obj)
		{
			valueList.add(o);
		}
	}
	
	/**
	 * getValueAt
	 * returns the value from a specified position
	 * 
	 * @param pos
	 * @return
	 */
	public Object getValueAt(int pos)
	{
		return valueList.get(pos);
	}
	
	/**
	 * getValues
	 * returns all the kv2 values
	 * 
	 * @return
	 */
	public ArrayList<Object> getValues()
	{
		return valueList;
	}
	
	/**
	 * toString
	 * returns the key value pair 2 as a string
	 */
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(key);
		sb.append("|");
		
		//the values
		for(int i = 0; i < valueList.size(); i++)
		{
			if(i == valueList.size() - 1)
			{
				sb.append(valueList.get(i));
			}
			else
			{
				sb.append(valueList.get(i) + "-");
			}
		}
		
		
		return sb.toString();
	}
}
