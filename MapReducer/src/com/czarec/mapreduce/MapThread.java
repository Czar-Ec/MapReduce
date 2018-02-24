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
	
	/**
	 * run
	 * executes the thread
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//convert data chunks into mapped data
		flights = MapReduce.chunkToFlights(dc);
		
		switch(mapType)
		{
			//count flights
			case 0:
				{
					//make a key value pair for each flight
					for(Flights f: flights)
					{
						map.put(f.getOriginAirportCode(), f.getFlightID());
						//System.out.println(f.getOriginAirportCode() + " " + f.getFlightID());
					}
					
					//print all mapped values to a file
					folderSetup("res\\task1kv1\\", 1);
					folderSetup("res\\task1kv2\\", 2);
				}
				break;
				
			//list flights
			case 1:
				{					
					//map the flights
					//BY THE FLIGHT ID
					//reducer groups the flights by the flight id
					for(Flights f : flights)
					{
						map.put(f.getFlightID(), f);
						//System.out.println(f.toString());
					}
					
					//print mapped values
					folderSetup("res\\task2kv1\\", 1);
					folderSetup("res\\task2kv2\\", 2);
					
				}
				break;
				
			//calculate num of passengers
			case 2:
				//map with passenger as value
				for(Flights f : flights)
				{
					map.put(f.getFlightID(), f.getPassengerID());
					//System.out.println(f.toString());
				}
				
				//print mapped values
				folderSetup("res\\task3kv1\\", 1);
				folderSetup("res\\task3kv2\\", 2);
				
				break;
				
			default:
				System.out.println("Error: Invalid map task");
				break;
		
		}
				
		
	}
	
	/**
	 * folderSetup
	 * function that sets up the files to be printed
	 */
	private void folderSetup(String fileLoc, int keyType)
	{
		////////////////////////////////////////////////////////////////////////////////////////////
		/*
		 * Making k1v1 pairs
		 */
		////////////////////////////////////////////////////////////////////////////////////////////
		String folder = new File(fileLoc).getAbsolutePath();
		File f = new File(folder);
		try
		{
			//check if the folder already exists
			if(!f.exists())
			{
				boolean created = f.mkdir();
				if(!created)
				{
					System.out.println("Error: folder cannot be created in res folder");
				}
			}
			else
			{
				switch(keyType)
				{
					case 1:
						printKV1(fileLoc);
						break;
						
					case 2:
						printKV2(fileLoc);
						break;
					
					default:
						System.out.println("Invalid key type");
						break;
				}
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
	private void printKV1(String fileLoc) throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter pw = new PrintWriter(fileLoc + "" + Thread.currentThread().getName(), "UTF-8");
		
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
	
	/**
	 * printKV2
	 * function that prints the kv2 to the kv2 folder
	 * 
	 * @param fileLoc
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	private void printKV2(String fileLoc) throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter pw = new PrintWriter(fileLoc + Thread.currentThread().getName(), "UTF-8");
		
		//get the kv2 pairs
		ArrayList<KeyValuePair2> kv2 = new ArrayList<KeyValuePair2>();
		kv2 = new Combiner(map).getKV2();
		
		//print all the key value pairs
		for(KeyValuePair2 addObj : kv2)
		{
			pw.append(addObj.toString());
			//adding a new line
			pw.append("\n");
		}
		
		pw.close();
	}
}



























