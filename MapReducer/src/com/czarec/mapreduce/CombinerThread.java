package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class CombinerThread implements Runnable {

	//hold the file that will need to be parsed
	File chunk;
	
	//temporarily stores kv1
	ArrayList<KeyValuePair1> kv = new ArrayList<KeyValuePair1>();
	
	//list of lists where each inner list has the key at position 0
	//and any other values are the values for the key
	ArrayList<ArrayList<Object>> kv2 = new ArrayList<ArrayList<Object>>();
	
	/**
	 * CombinerThread default constructor
	 */
	CombinerThread() {}
	
	CombinerThread(File f)
	{
		chunk = f;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try
		{
			//read the chunk
			BufferedReader br = new BufferedReader(new FileReader(chunk));
			
			//line buffer
			String lineBuf;
			
			while((lineBuf = br.readLine()) != null)
			{
				//get the key value pairs
				String[] str = lineBuf.split("\\|");
				
				KeyValuePair1 keyValue = new KeyValuePair1(str[0], str[1]);
				kv.add(keyValue);
			}
			
			//make a list of lists
			for(KeyValuePair1 keyValues : kv)
			{
				if(kv2.size() == 0)
				{
					ArrayList<Object> obj = new ArrayList<Object>();
					obj.add(keyValues.getKey());
					obj.add(keyValues.getValue());
					kv2.add(obj);
				}
				else
				{
					//for each key value 1, check if a key exists in the list
					for(ArrayList<Object> check : kv2)
					{
						//if matching keys then add a value to the key
						if(keyValues.getKey().equals(check.get(0)))
						{
							check.add(keyValues.getValue());
						}
						else
						{
							
						}
					}
				}
				
			}
			
			folderSetup();
		}
		catch (Exception e)
		{
			
		}
	}

	private void folderSetup()
	{
		////////////////////////////////////////////////////////////////////////////////////////////
		/*
		 * Making k2v2 pairs
		 */
		////////////////////////////////////////////////////////////////////////////////////////////
		String folder = new File("res\\kv2").getAbsolutePath();
		File f = new File(folder);
		try
		{
			//check if folder exists
			if(!f.exists())
			{
				boolean created = f.mkdir();
				if(!created)
				{
					System.out.println("Error: kv2 folder cannot be created in res folder");
				}
			}
			else
			{
				printKV2();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void printKV2() throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter pw = new PrintWriter("res\\kv2\\" + Thread.currentThread().getName(), "UTF-8");
		
		for(Object obj : kv2)
		{
			System.out.println(obj);
		}
		
		pw.close();
	}
}
