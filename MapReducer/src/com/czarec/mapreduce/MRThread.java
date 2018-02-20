package com.czarec.mapreduce;

import java.util.ArrayList;

public class MRThread implements Runnable {

	//variable to know which value is being mapped
	//1 = Task1
	//2 = Task2
	//3 = Task3
	private volatile int mapType;
	
	private volatile Map m = new Map();
	private volatile DataChunk dc;
	private volatile ArrayList<Flights> flights = new ArrayList<Flights>();
	
	Combiner c;
	
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
			m.put(f.getOriginAirportCode(), f.getFlightID());
			//System.out.println(f.getOriginAirportCode() + " " + f.getFlightID());
		}
		
		c = new Combiner(m.get());
		c.printList();
	}

	
	public Map getKeyValues()
	{
		return m;
	}
	
	
}
