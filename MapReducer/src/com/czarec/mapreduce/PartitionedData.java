package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * class that takes in the 
 * 
 * @author User
 *
 */
public class PartitionedData {

	//Arraylist to store the data chunks
	private List<DataChunk> dataChunkList = new ArrayList<DataChunk>();
	
	//chunk size
	private int chunkSize;
	
	/**
	 * PartitionedData
	 * default constructor
	 */
	PartitionedData(){}
	
	/**
	 * Partitioned data
	 * class will automatically split the data file into chunks
	 * (chunks not by data size but by line size, emulates the concept
	 * but not as accurate as the hadoop version)
	 * 
	 * @param filePath
	 * @param _chunkSize
	 */
	PartitionedData(String filePath, int _chunkSize) throws Exception
	{
		chunkSize = _chunkSize;
		
		try
		{
			//read file
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			
			//line buffer
			String lineBuf;
			
			//Data chunk class
			DataChunk dc = new DataChunk(chunkSize);
			
			//read the file and make a new chunk after _chunkSize lines
			while((lineBuf = br.readLine()) != null)
			{
				//if the data chunk is not full
				if(dc.add(lineBuf))
				{
					//debug
					//System.out.println(lineBuf);
				}
				else
				{
					//add the full chunk to the chunk list
					dataChunkList.add(dc);
					
					//create new data chunk and add the line that could not be added
					dc = new DataChunk(chunkSize);
					dc.add(lineBuf);
				}
			}
			
			//check if there are any leftover data that need to be added to the list
			if(dc.getChunkListSize() > 0)
			{
				dataChunkList.add(dc);
			}
			
			br.close();
			
		}
		catch(Exception e)
		{
			
		}
	}
	
	/**
	 * getChunkSize
	 * returns the chunk size set for the paritioned data class
	 * 
	 * @return chunkSize
	 */
	public int getChunkSize() { return chunkSize; }
	
	/**
	 * getChunkAt
	 * gets a specific chunk at a specfied position
	 * 
	 * @param chunk
	 * @return returnObj
	 */
	public DataChunk getChunkAt(int chunk)
	{
		DataChunk returnObj = null;
		
		//check if the chunk exists
		if(chunk <= dataChunkList.size())
		{
			returnObj = dataChunkList.get(chunk);
		}
		
		return returnObj;
	}
	
	/**
	 * getAllChunks
	 * gets all the chunks stored in this partitioned data class
	 * 
	 * @return chunkList
	 */
	public List<DataChunk> getAllChunks() 
	{ 
		//debug
		//System.out.println(chunkList);
		return dataChunkList; 
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
