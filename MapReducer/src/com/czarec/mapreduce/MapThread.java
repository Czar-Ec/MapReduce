package com.czarec.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 * class that is executed for the Map thread
 * 
 * @author Czar Echavez
 */
public class MapThread implements Runnable {

	//variable to know which value is being mapped
	//1 = Task1
	//2 = Task2
	//3 = Task3
	private volatile int mapType;
	
	private volatile Map map = new Map();
	private volatile DataChunk dc;
	private volatile ArrayList<Flights> flights = new ArrayList<Flights>();
	
	/**
	 * MRThread
	 * default constructor
	 */
	MapThread() {}
	
	/**
	 * MRThread
	 * constructor which requires a data chunk to work off from
	 * @param data
	 */
	MapThread(DataChunk data, int mt)
	{
		dc = data;
		mapType = mt;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//convert data chunks into mapped data
		switch(mapType)
		{
			//count flights
			case 0:
				flights = MapReduce.chunkToFlights(dc);
				
				//make a key value pair for each flight
				for(Flights f: flights)
				{
					map.put(f.getOriginAirportCode(), f.getFlightID());
					//System.out.println(f.getOriginAirportCode() + " " + f.getFlightID());
				}
				break;
				
			//list flights
			case 1:
				
				break;
				
			//calculate num of passengers
			case 2:
				break;
				
			default:
				System.out.println("Error: Invalid map task");
				break;
		
		}
				
		//print all mapped values to a file
		folderSetupKV1();
		folderSetupKV2();
	}
	
	/**
	 * folderSetup
	 * function that sets up the files to be printed
	 */
	private void folderSetupKV1()
	{
		////////////////////////////////////////////////////////////////////////////////////////////
		/*
		 * Making k1v1 pairs
		 */
		////////////////////////////////////////////////////////////////////////////////////////////
		String folder = new File("res\\kv1").getAbsolutePath();
		File f = new File(folder);
		try
		{
			//check if the folder already exists
			if(!f.exists())
			{
				boolean created = f.mkdir();
				if(!created)
				{
					System.out.println("Error: kv1 folder cannot be created in res folder");
				}
			}
			else
			{
				printKV1();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}		
	}
	
	/**
	 * printKV1
	 * function that prints the kv1 to the kv1 folder
	 * 
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	private void printKV1() throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter pw = new PrintWriter("res\\kv1\\" + Thread.currentThread().getName(), "UTF-8");
		
		//print every key value pair
		ArrayList<Object> o = map.get();
		
		//print all the key value pairs
		for(int i = 0; i < o.size(); i++)
		{
			//convert to key value pair to print its individual key and value per line
			KeyValuePair1 kv = (KeyValuePair1) o.get(i);
			pw.println(kv.getKey() + "|" + kv.getValue());
		}
		
		pw.close();
	}
	
	private void folderSetupKV2()
	{
		////////////////////////////////////////////////////////////////////////////////////////////
		/*
		 * Making k2v2 pairs
		 */
		////////////////////////////////////////////////////////////////////////////////////////////
		
		
	}
}



























