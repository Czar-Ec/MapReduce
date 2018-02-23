package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
	 * store the data chunks for each of the input files
	 */
	///////////////////////////////////////////////////////////////////////////////////
	static List<DataChunk> chunkFile1 = new ArrayList<DataChunk>();
	static List<DataChunk> chunkFile2 = new ArrayList<DataChunk>();
	
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
			inputFile1 = "res\\passenger.csv";
			inputFile2 = "res\\airports.csv";
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
		
		try
		{
			//partition the data files
			PartitionedData pd = new PartitionedData(inputFile1, 64);
			chunkFile1 = pd.getAllChunks();
			
			PartitionedData pd1 = new PartitionedData(inputFile2, 64);
			chunkFile2 = pd1.getAllChunks();
		}
		catch(Exception e)
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
						//airportList.add(a);
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
	 * @throws InterruptedException 
	 */
	public static void countFlights() throws InterruptedException
	{
		//add threads to a list
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		
		//make a new thread for each chunk
		for(int i = 0; i < chunkFile1.size(); i++)
		{
			MapThread mrt = new MapThread(chunkFile1.get(i), 0);
			Thread t = new Thread(mrt);
			t.setName("chunk"+i);
			threadList.add(t);
			
			//run the thread
			threadList.get(i).start();
		}
		
		//join all threads
		for(Thread t : threadList)
		{
			t.join();
		}
		
		//combine all the values from the kv1 pairs
		new Combiner();
		
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
	 * functions that make the program work e.g. the validation checks
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
	public static boolean validation(String str, int validationType)
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
	
	/**
	 * chunkToFlights
	 * takes in datachunks and converts them into a list of flights
	 * 
	 * @param dc
	 * @return flights
	 */
	public static ArrayList<Flights> chunkToFlights(DataChunk dc)
	{
		ArrayList<Flights> flights = new ArrayList<Flights>();
		
		ArrayList<Object> dcList = dc.getChunk();
		
		for(int i = 0; i < dcList.size(); i++)
		{
			//split the file
			String[] str = ((String) dcList.get(i)).split(",");
			
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
					flights.add(f);
				}
				else
				{
					//print error to console
					System.out.print("Error in line " + dcList.get(i) + "\t invalid: ");
					
					//figure out which error was caused
					if(!isFlightNumValid)
						System.out.print("\tflight_number(" + str[0] + ") ");
					
					if(!isPassNumValid)
						System.out.print("\tpassenger_number(" + str[1] + ") ");
					
					if(!isDestValid)
						System.out.print("\tdestination_airport(" + str[2] + ") ");

					if(!isOriginValid)
						System.out.print("\torigin_airport(" + str[3] + ") ");
					
					System.out.println("");
				}
			}
			else
			{
				//print error to console
				System.out.println("Error in line " + dcList.get(i));
			}
		}
		
		return flights;
	}

	/**
	 * fileOutput
	 * function that allows for printing to the output file
	 * @param out
	 * @param fw
	 */
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



















