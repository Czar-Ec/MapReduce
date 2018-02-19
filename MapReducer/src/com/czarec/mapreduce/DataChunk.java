package com.czarec.mapreduce;

import java.util.ArrayList;

/**
 * DataChunk
 * class that just holds a chunk of data
 * @author User
 *
 */
public class DataChunk {

	//variable that stores objects to be stored by this chunk
	private ArrayList<Object> chunk = new ArrayList<Object>();
	
	private int chunkSize;
	
	
	/**
	 * DataChunk
	 * empty constructor
	 */
	DataChunk() { chunkSize = 64; }
	
	/**
	 * DataChunk
	 * constructor that specifies the chunkSize
	 * @param chunkSize
	 */
	DataChunk(int _chunkSize) { chunkSize = _chunkSize; }
	
	/**
	 * add
	 * add a new value to the chunk array
	 * 
	 * @param o
	 * @return success
	 */
	public boolean add(Object o) 
	{ 
		boolean success = false;
		
		//check if the size of the chunk is exceeded
		if(!(chunk.size() + 1 > chunkSize))
		{
			//add the object
			chunk.add(o); 
			success = true;
		}
		
		return success;
	}
	
	/**
	 * printVal
	 * prints all the values of the chunk to the screen
	 */
	public void printVal()
	{
		int line = 0;
		for(Object o : chunk)
		{
			System.out.println(line + " " + o);
			line++;
		}
	}
	
	/**
	 * getChunk
	 * returns all the values of the chunk
	 * 
	 * @return chunk
	 */
	public ArrayList<Object> getChunk()
	{
		return chunk;
	}
	
	/**
	 * clearChunk
	 * removes all values of the chunk	 * 
	 */
	public void clearChunk() { chunk.clear(); }
	
	/**
	 * getChunkSize
	 * returns the size of the chunk list
	 * 
	 * @return chunk.size()
	 */
	public int getChunkListSize() { return chunk.size(); }
}