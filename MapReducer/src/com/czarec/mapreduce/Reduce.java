package com.czarec.mapreduce;

import java.io.File;
import java.util.ArrayList;

public class Reduce {

	//store the key values
	ArrayList<KeyValuePair2> tmpkv2 = new ArrayList<KeyValuePair2>();
	ArrayList<KeyValuePair2> finalkv2 = new ArrayList<KeyValuePair2>();
	
	
	/**
	 * Reduce
	 * default constructor
	 */
	Reduce() {}
	
	/**
	 * Reduce
	 * the reducer of the program that combines everything
	 * 
	 * @param taskLoc
	 */
	Reduce(String taskLoc) 
	{
		//list of threads
		ArrayList<ReduceThread> rThreadList = new ArrayList<ReduceThread>();
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		
		//check for files in the kv2 folder
		File folder = new File(taskLoc);
		File[] folderContents = folder.listFiles();
				
		//make a thread for each file in the kv2 folder
		for(int i = 0; i < folderContents.length; i++)
		{
			if(folderContents[i].isFile())
			{
				ReduceThread rt = new ReduceThread(folderContents[i]);
				Thread t = new Thread(rt);
				t.setName(folderContents[i].getName());
				rThreadList.add(rt);
				threadList.add(t);
				
				//start the thread
				threadList.get(i).start();
			}
		}
		
		//join all threads
		for(int i = 0; i < threadList.size(); i++)
		{
			try
			{
				threadList.get(i).join();
				
				//get the key value pairs
				tmpkv2.addAll(rThreadList.get(i).getKV2());
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		//remove duplicates
		for(KeyValuePair2 dup : tmpkv2)
		{
			//boolean to check if a key already exists
			boolean exists = false;
			
			//loop through the final list
			for(KeyValuePair2 compare : finalkv2)
			{
				//if key already exists, add all the values from duplicate 
				//to the one already in list
				if(compare.getKey().equals(dup.getKey()))
				{
					compare.addList(dup.getValues());
					exists = true;
				}
			}
			
			//if not found
			if(!exists)
			{
				ArrayList<Object> values = new ArrayList<Object>();
				values.addAll(dup.getValues());
				
				KeyValuePair2 newObj = new KeyValuePair2(dup.getKey(), values);
				finalkv2.add(newObj);
			}
		}
		
		//debug
//		for(KeyValuePair2 kvList : finalkv2)
//		{
//			System.out.println(kvList.toString());
//		}
		
		
	}
	
	/**
	 * getKV2
	 * returns the kv2 pairs
	 * 
	 * @return finalkv2
	 */
	public ArrayList<KeyValuePair2> getKV2()
	{
		return finalkv2;
	}
}
