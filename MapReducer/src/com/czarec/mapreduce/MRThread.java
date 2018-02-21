package com.czarec.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;



public class MRThread implements Runnable {

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
	MRThread() {}
	
	/**
	 * MRThread
	 * constructor which requires a data chunk to work off from
	 * @param data
	 */
	MRThread(DataChunk data, int mt)
	{
		dc = data;
		mapType = mt;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//convert data chunks into flights
		flights = MapReduce.chunkToFlights(dc);
		
		//make a key value pair for each flight
		for(Flights f: flights)
		{
			map.put(f.getOriginAirportCode(), f.getFlightID());
			//System.out.println(f.getOriginAirportCode() + " " + f.getFlightID());
		}
		
		//print all mapped values to a file
		folderSetup();
	}
	
	private void folderSetup()
	{
		String folder = new File("res\\chunks").getAbsolutePath();
		File f = new File(folder);
		try
		{
			//check if the folder already exists
			if(!f.exists())
			{
				boolean created = f.mkdir();
				if(!created)
				{
					System.out.println("Error: chunks folder cannot be created in res folder");
				}
			}
			else
			{
				printToFile();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void printToFile() throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter pw = new PrintWriter("res\\chunks\\" + Thread.currentThread().getName(), "UTF-8");
		
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
}
