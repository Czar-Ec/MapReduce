package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Main class
 * the entry point of the program which holds the main
 * 
 * I will also cite the program which I used to base this one on
 * https://github.com/cilliand/CloudComputingCoursework-MSc
 * Seems to be doing almost the exact same thing as the coursework brief asks
 * but some parts are different
 * 
 * @author Czar Ian Echavez
 *
 */
public class MapReduce {

	//input files and outputs specified via the user in console (hardcoded in debug)
	static String newLine = System.getProperty("line.separator"); //new line
	static String inputFile1 = "",
			inputFile2 = "",
			outputFile = "";
	
	//file writing
	static FileWriter fw;
	
	///////////////////////////////////////////////////////////////////////////////////
	/*
	 * arraylist of airports and flights
	 * the source was very inefficient because it required reading the files multiple 
	 * times, instead I've stored the read values so files will only be read once 
	 */
	///////////////////////////////////////////////////////////////////////////////////
	static ArrayList<Flights> flightList = new ArrayList<Flights>();
	static ArrayList<Airport> airportList = new ArrayList<Airport>();
	
	///////////////////////////////////////////////////////////////////////////////////
	/*
	 * Values used as constants for validation checks
	 */
	///////////////////////////////////////////////////////////////////////////////////
	private static final int 
	VALIDATE_FLIGHT_NUM = 1,
	VALIDATE_PASSENGER_NUM = 2,
	VALIDATE_AIRPORT_CODE = 3;
	
	
	
	/**
	 * main function
	 * the argumenta passed are the files that will be used as file source
	 * @param args
	 */
	public static void main(String[] args) throws FileNotFoundException{
		// TODO Auto-generated method stub
		
		try
		{
			//inputFile1 = args[0];
			//inputFile2 = args[1];
			//outputFile = args[2];
			
			//debug values for now
			inputFile1 = "res\\AComp_Passenger_data.csv";
			inputFile2 = "res\\Top30_airports_LatLong.csv";
			outputFile = "res\\output.txt";
			
			
		}
		catch (Exception e)
		{
			//output to say if files are missing
			System.out.println(
			"Files not found\n" +
			"input file 1: " + inputFile1 + "\n" +
			"input file 2: " + inputFile2 + "\n" +
			"output file: " + outputFile + "\n"					
					);
			
			System.exit(0);
		}
		
		///////////////////////////////////////////////////////////////////////////////////
		/*
		 * read file 1 and store the list of flights as Flight objects
		 */
		///////////////////////////////////////////////////////////////////////////////////
		try 
		{
			//read the file
			BufferedReader b = new BufferedReader(new FileReader(inputFile1));
			
			//line buffer
			String lineBuffer;

			//loop through the entire file
			int lineCount = 1;
			while((lineBuffer = b.readLine()) != null)
			{
				//debug the line output
				//System.out.println(lineBuffer);
				
				//split the file
				String[] str = lineBuffer.split(",");
				
				//check if the line is missing some values
				if(str.length >= 6)
				{
					//validate values
					boolean
					isFlightNumValid = validation(str[0], VALIDATE_FLIGHT_NUM),
					isPassNumValid = validation(str[1], VALIDATE_PASSENGER_NUM),
					isDestValid = validation(str[2], VALIDATE_AIRPORT_CODE),
					isOriginValid = validation(str[3], VALIDATE_AIRPORT_CODE);
					
					//if passed all checks, make and add flight to flight list
					if(isFlightNumValid && isPassNumValid &&
						isDestValid && isOriginValid)
					{
						//create the new flight with the inputs
						Flights f = new Flights(str[0], str[1], str[2], str[3], str[4], str[5]);
						
						//debug
						//System.out.println(f.toString());
						
						//add to flight list
						flightList.add(f);
					}
					else
					{
						//print error to console
						System.out.print("Error in line " + lineCount + " invalid: ");
						
						//figure out which error was caused
						if(!isFlightNumValid)
							System.out.print("flight_number(" + str[0] + ") ");
						
						if(!isPassNumValid)
							System.out.print("passenger_number(" + str[1] + ") ");
						
						if(!isDestValid)
							System.out.print("destination_airport(" + str[2] + ") ");

						if(!isOriginValid)
							System.out.print("origin_airport(" + str[3] + ") ");
						
						System.out.println("");
					}
				}
				else
				{
					//print error to console
					System.out.println("Error in line " + lineCount + " in file " + inputFile1);
				}
				
				//helps with debugging file
				lineCount++;
			}
			
			//close file
			b.close();
			
			System.out.println();
			System.out.println("=============================================================");
			System.out.println();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		///////////////////////////////////////////////////////////////////////////////////
		/*
		 * read file 2 and store list of airports as Airport objects
		 */
		///////////////////////////////////////////////////////////////////////////////////
		try
		{
			//read the file
			BufferedReader b = new BufferedReader(new FileReader(inputFile2));
			
			//line buffer
			String lineBuffer;
			
			//loop through the entire file
			int lineCount = 1;
			while((lineBuffer = b.readLine()) != null)
			{
				//debug
				//System.out.println(lineBuffer);
				
				//split the file
				String[] str = lineBuffer.split(",");
				
				//check if the line is missing some values
				if(str.length >= 4)
				{
					//only one validation, which is the airport code
					if(validation(str[1], VALIDATE_AIRPORT_CODE))
					{
						//create airport with the inputs
						Airport a = new Airport(str[0], str[1], str[2], str[3]);
						
						//debug
						//System.out.println(a.toString());
						
						//add to airport list
						airportList.add(a);
					}
					else
					{
						//print error to console
						System.out.println("Error in line " + lineCount + " invalid: airport_code(" + str[1] + ") ");
					}
					
					
				}
				else
				{
					//print error to console
					System.out.println("Error in line " + lineCount + " in file " + inputFile2);
				}
				
				//helps with debugging file
				lineCount++;
			}
			
			//close file
			b.close();
			
			System.out.println();
			System.out.println("=============================================================");
			System.out.println();			
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		//run the tasks
		try
		{
			countFlights();
			
			flightList();
			
			calculatePassengers();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	

	///////////////////////////////////////////////////////////////////////////////////
	/*
	 * This program is split into different parts so that it is much clearer to see
	 * which part of the program does what part is being asked of by the coursework
	 * specifications
	 */
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * countFlights
	 * determines the number of flights from each airport
	 * also lists the airports that are not used
	 */
	public static void countFlights()
	{
		//map all the values
		Map map = new Map();
		
		for(int i = 0; i < flightList.size(); i++)
		{
			//map the origin airport
			//calculating the flight FROM each airport
			map.put(flightList.get(i));
		}
		
		//debug kvpairs
		/*ArrayList<KeyValuePair1> kv1Debug = new ArrayList<>();
		kv1Debug = map.get();
		for(int i = 0; i < kv1Debug.size(); i++)
		{
			System.out.println(kv1Debug.get(i).toString());
		}*/
		
		//implement a combiner
		Combiner c = new Combiner(map.get());
		
		
	}
	
	/**
	 * flightList
	 * creates a list of flights based on flight ID
	 * includes the passenger Id, relevant IATA/FAA codes, the departure time, 
	 * the arrival time, and the flight times
	 */
	public static void flightList()
	{
		
	}
	
	/**
	 * calculatePassengers
	 * calculates the number of passengers for each flight
	 */
	public static void calculatePassengers()
	{
		
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	/*
	 * functions that make the program work i.e. the validation checks
	 */
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * validation
	 * validates and input according to validation type
	 * Validation types:
	 * VALIDATE_FLIGHT_NUM = 1
	 * VALIDATE_PASSENGER_NUM = 2
	 * VALIDATE_AIRPORT_CODE = 3
	 * 
	 * @param str
	 * @param validationType
	 * @return valid
	 */
	private static boolean validation(String str, int validationType)
	{
		//value to be returned
		boolean valid = false;
		
		//the different types of validation
		switch(validationType)
		{
			//validating flight number
			case 1:
				//format for flight number is XXXnnnnXXn
				//where X is a capital letter and n is an integer between 0-9
				if(str.matches("[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]{1}"))
				{
					valid = true;
				}
				break;
			
			//validating passenger number
			case 2:
				//format for passenger number is XXXnnnnX
				//where X is a capital letter and n is an integer between 0-9
				if(str.matches("[A-Z]{3}[0-9]{4}[A-Z]{1}"))
				{
					valid = true;
				}
				break;
				
			//validating airport code
			case 3:
				//format for airport code is XXX all capitals letter only
				if(str.matches("[A-Z]{3}"))
				{
					valid = true;
				}
				break;
			
			
			//if task type isnt matched it will just return false
			default:
				valid = false;
				break;
		}
		
		return valid;
	}

	private static void fileOutput(String out, FileWriter fw)
	{
		//output to the file
		try
		{
			fw.write(out);
			System.out.print(out);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}



















