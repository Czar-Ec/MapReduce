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
	
	/**
	 * Combiner
	 * constructor
	 */
	Combiner() 
	{
		//check if there are key values available
		String fileLoc = new File("res\\kv1").getAbsolutePath();
		File f = new File(fileLoc);
		
		//get all the files in the folder
		ArrayList<File> fileList = new ArrayList<File>();
		
		try
		{
			if(!f.exists())
			{
				System.out.println("Error: no key values to combine");
			}
			else
			{
				
				File[] folderContents = new File("res\\kv1").listFiles();
				
				for(File file : folderContents)
				{
					//only files, not folders
					if(file.isFile())
					{
						fileList.add(file);
					}
				}
				
				//make a thread for each file
				ArrayList<Thread> threadList = new ArrayList<Thread>();
				
				for(int i = 0; i < fileList.size(); i++)
				{
					CombinerThread ct = new CombinerThread(fileList.get(i));
					Thread t = new Thread(ct);
					t.setName(fileList.get(i).getName());
					threadList.add(t);
					
					//run the thread
					threadList.get(i).start();
				}
				
				
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		//create a thread for each file that exists from the kv1 folder
		//and delete them to stop duplicate data
		
		
		//the combiner stores the values in an array and remove duplicates by turning them into kv2
		
		//then print out the kv2
	}
	
	
}


















