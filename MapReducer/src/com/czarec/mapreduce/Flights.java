package com.czarec.mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Flights {

	//passenger ID
	private String passengerID;
	
	//flight ID
	private String flightID;
	
	//origin
	private String originAirportCode;
	
	//destination
	private String destinationAirportCode;
	
	/**
	 * Departure time of flight
	 * this is in Unix format
	 */
	private String departureTime;
	
	/**
	 * flight time
	 * aka flight length
	 */
	private int flightTime;
	
	/**
	 * Flights default constructor
	 */
	Flights()
	{
		//lel xd
	}
	
	/**
	 * Flight main constructor
	 * used to create Flights with values
	 * 
	 * @param pID
	 * @param fID
	 * @param origin
	 * @param destination
	 * @param depTime
	 * @param fTime
	 */
	Flights(String pID, String fID, String origin, String destination, String depTime, String fTime)
	{
		//set values
		passengerID = pID;
		flightID = fID;
		originAirportCode = origin;
		destinationAirportCode = destination;
		
		//parse flight time into int
		flightTime = Integer.parseInt(fTime);
		
		//converts departure time to unix format
		try
		{
			departureTime = convertFromUnix(Long.parseLong(depTime));
		}
		catch(Exception e)
		{
			departureTime = depTime;
		}
		
	}
	
	/**
	 * toString
	 * returns a string which can be output to console
	 * has all the data + labels
	 */
	public String toString()
	{
		return flightID + "," + passengerID + "," + destinationAirportCode + "," + originAirportCode + "," + departureTime + "," + flightTime;
	}
	
	public String printFormat()
	{
		return "Passenger ID: " + flightID + "\n" +
				"Flight ID : " + passengerID + "\n" +
				"Origin: " + originAirportCode + "\n" +
				"Destination: " + destinationAirportCode + "\n" + 
				"Departure Time: " + departureTime + "\n" + 
				"Flight Length: " + flightTime / 60 + " hours " + flightTime % 60 +  " minutes\n\n";
	}
	
	/**
	 * convertFromUnix
	 * takes in a unix value and converts it into date format
	 * 
	 * @param unixTime
	 * @return
	 */
	private String convertFromUnix(long unixTime)
	{
		//convert time to seconds
		long unixSecs = unixTime * 1000L;
		
		//converting to date time format
		Date d = new Date(unixSecs);
		SimpleDateFormat s = new SimpleDateFormat("HH:mm:ss");
		
		String formatted = s.format(d);
		return formatted;
	}

	///////////////////////////////////////////////////////////////////////////////////
	//getters
	
	/**
	 * getPassengerID
	 * returns the passengerID
	 * 
	 * @return passengerID
	 */
	public String getPassengerID() { return passengerID; }
	
	/**
	 * getFlightID
	 * returns flightID
	 * 
	 * @return flightID
	 */
	public String getFlightID() { return flightID; }
	
	/**
	 * getOriginAirportCode
	 * returns airport code of the origin
	 * 
	 * @return originAirportCode
	 */
	public String getOriginAirportCode() { return originAirportCode; }
	
	/**
	 * getDestAirportCode
	 * returns airport code of the destination
	 * 
	 * @return destinationAirportCode
	 */
	public String getDestAirportCode() { return destinationAirportCode; }
	
	/**
	 * getDeparture
	 * returns time of departure
	 * 
	 * @return departureTime
	 */
	public String getDeparture() { return departureTime; }
	
	/**
	 * getFlightTime
	 * returns flight time length
	 * 
	 * @return flightTime
	 */
	public int getFlightTime() { return flightTime; }

	///////////////////////////////////////////////////////////////////////////////////
	
}
