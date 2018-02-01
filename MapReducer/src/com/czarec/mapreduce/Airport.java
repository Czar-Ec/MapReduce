package com.czarec.mapreduce;

public class Airport {
	
	//airport name
	private String airportName;
	
	//airport code
	private String airportCode;
	
	//airport location - longtitude and lattitude
	private double longtitude, latitude;

	
	
	
	
	
	/**
	 * Airport default constructor
	 */
	Airport()
	{
		//lel xd
	}
	
	
	/**
	 * Airport main constructor
	 * used to create an airport class with values
	 * @param name
	 * @param code
	 * @param lat
	 * @param lon
	 */
	Airport(String name, String code, String lat, String lon)
	{
		//set values
		airportName = name;
		airportCode = code;
		
		//set longtitude and latitude but in double format
		longtitude = Double.parseDouble(lon);
		latitude = Double.parseDouble(lat);
	}
	
	/**
	 * toString
	 * returns a string which can be printed to console.
	 * the string contains the airport's code, name and long + lat
	 */
	public String toString()
	{
		return airportCode + " | " + airportName + "\n" +
				"longtitude: " + longtitude + "\n" +
				"latitude: " + latitude + "\n\n";
	}

	///////////////////////////////////////////////////////////////////////////////////
	//getters

	/**
	 * getCode
	 * returns airport code
	 * 
	 * @return airportCode
	 */
	public String getCode() { return airportCode; }
	
	/**
	 * getName
	 * returns airport name
	 * 
	 * @return airportName
	 */
	public String getName() { return airportName; }
	
	/**
	 * getLongtitude
	 * returns airport longtitude
	 * 
	 * @return longtitutde
	 */
	public double getLongtitude() { return longtitude; }
	
	/**
	 * getLatitude
	 * returns airport latitude
	 * 
	 * @return latitude
	 */
	public double getLatitude() { return latitude; }
	
	
	///////////////////////////////////////////////////////////////////////////////////
}
