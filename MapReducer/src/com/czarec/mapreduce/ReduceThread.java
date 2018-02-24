package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ReduceThread implements Runnable {

	//file to be parsed by this thread
	File process;
	
	
	ArrayList<KeyValuePair2> kv2 = new ArrayList<KeyValuePair2>();
	
	/**
	 * ReduceThread
	 * empty constructor
	 */
	ReduceThread() {}
	
	/**
	 * ReduceThread
	 * the thread that executes the reduce functions i.e. converts the kv2 files into key value pair 2 objects
	 * 
	 * @param f
	 */
	ReduceThread(File f)
	{
		process = f;
	}
	
	@Override
	public void run() {
		
		//read the file and convert the values in kv2
		try 
		{
			BufferedReader br = new BufferedReader(new FileReader(process));
			
			//line buffer
			String lineBuf;
			//read through the file
			while((lineBuf = br.readLine()) != null)
			{
				//get the key from the key value
				String[] keyValueSplit = lineBuf.split("\\|");
				
				//get the values
				ArrayList<Object> valuesList = new ArrayList<Object>();
				String[] valueSplit = keyValueSplit[1].split("-");
				
				for(Object val : valueSplit)
				{
					//add each value to the value list
					valuesList.add(val);
				}
				
				//form the KeyValuePair2
				KeyValuePair2 addObj = new KeyValuePair2(keyValueSplit[0], valuesList);
				//add to list
				kv2.add(addObj);
				
				
			}
			
			br.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		
	}

	/**
	 * getKV2
	 * returns the key value pairs processed by the thread
	 * 
	 * @return kv2
	 */
	public ArrayList<KeyValuePair2> getKV2()
	{
		return kv2;
	}
	
}
