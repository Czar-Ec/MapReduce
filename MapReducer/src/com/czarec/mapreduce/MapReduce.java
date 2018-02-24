package com.czarec.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
	
	static ArrayList<Airport> airportList = new ArrayList<Airport>();
	
	///////////////////////////////////////////////////////////////////////////////////
	/*
	 * Values used as constants for validation checks
	 */
	///////////////////////////////////////////////////////////////////////////////////
	private static final int 
	VALIDATE_FLIGHT_NUM = 1,
	VALIDATE_PASSENGER_NUM = 2,
	VALIDATE_AIRPORT_CODE = 3,
	VALIDATE_AIRPORT_NAME = 4,
	VALIDATE_COORDINATES = 5;
	
	
	
	/**
	 * main function
	 * the argumenta passed are the files that will be used as file source
	 * @param args
	 */
	public static void main(String[] args) throws FileNotFoundException{
		// TODO Auto-generated method stub
		
		try
		{
			if(args.length == 0)
			{
				System.out.println("No arguments detected, using test files instead");
				
				inputFile1 = "res\\test1.csv";
				inputFile2 = "res\\test2.csv";
				outputFile = "res\\outputTest.txt";
			}
			else
			{
				inputFile1 = args[0];
				inputFile2 = args[1];
				outputFile = args[2];
			}
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
					if(validation(str[0], VALIDATE_AIRPORT_NAME) && validation(str[1], VALIDATE_AIRPORT_CODE) 
							&& validation(str[2], VALIDATE_COORDINATES) && validation(str[3], VALIDATE_COORDINATES))
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
			//output file setup
			fw = new FileWriter(new File(outputFile));
			
			countFlights();
			
			flightList();
			
			calculatePassengers();
			
			//close the file
			fw.close();
			fw = null;
			
			//empty the folders unless argument states otherwise
			if(!(args.length == 0))
			{
				if(args[3].equals("true"))
				{
					emptyDirectory(new File("res\\task1kv1"));
					emptyDirectory(new File("res\\task1kv2"));
				}
				if(args[3].equals("true"))
				{
					emptyDirectory(new File("res\\task2kv1"));
					emptyDirectory(new File("res\\task2kv2"));
				}
				if(args[3].equals("true"))
				{
					emptyDirectory(new File("res\\task3kv1"));
					emptyDirectory(new File("res\\task3kv2"));
				}
			}
			else
			{
				System.out.println("\n\n ============================================ \n"
						+ "No arguments were detected, test files were used instead\n");
				
				emptyDirectory(new File("res\\task1kv1"));
				emptyDirectory(new File("res\\task1kv2"));
				emptyDirectory(new File("res\\task2kv1"));
				emptyDirectory(new File("res\\task2kv2"));
				emptyDirectory(new File("res\\task3kv1"));
				emptyDirectory(new File("res\\task3kv2"));
			}
			
			
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
	 * @throws IOException 
	 */
	public static void countFlights() throws InterruptedException, IOException
	{
		/////////////////////////////////////////////////////////////////////////////
		/*
		 * Map
		 */
		/////////////////////////////////////////////////////////////////////////////
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
			try
			{
				t.join();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		/////////////////////////////////////////////////////////////////////////////
		/*
		 * Reduce
		 */
		/////////////////////////////////////////////////////////////////////////////
		
		Reduce r = new Reduce("res\\task1kv2");
		
		ArrayList<Airport> unmatchedAirports = new ArrayList<Airport>();
		
		fileOutput("Flight count: \n", fw);
		
		//need to link this to airports to list which has how many flights
		//check for each airport the matching key and then print out the size of value list
		for(Airport a : airportList)
		{
			//boolean to check for matches
			boolean match = false;
			
			//check through the kv2
			for(KeyValuePair2 kv2 : r.getKV2())
			{
				if(a.getCode().equals(kv2.getKey()))
				{
					match = true;
					fileOutput(kv2.getKey() + ": " + kv2.getValues().size() + " \n", fw);
				}
			}
			
			//if no matches then airport not used
			if(!match)
			{
				unmatchedAirports.add(a);
			}
		}
		
		//formatting
		fileOutput("\n-----------------------", fw);
		fileOutput("Unmatched airports: \n", fw);
		
		//output unmatched airports
		for(Airport a : unmatchedAirports)
		{
			fileOutput(a.getCode() + " \n", fw);
		}
		
		fileOutput("\n-----------------------", fw);
		fileOutput("\n", fw);
	}
	
	/**
	 * flightList
	 * creates a list of flights based on flight ID
	 * includes the passenger Id, relevant IATA/FAA codes, the departure time, 
	 * the arrival time, and the flight times
	 * @throws IOException 
	 */
	public static void flightList() throws IOException
	{
		/////////////////////////////////////////////////////////////////////////////
		/*
		* Map
		*/
		/////////////////////////////////////////////////////////////////////////////
		ArrayList<Thread> threadList = new ArrayList<Thread>();
				
		//make a new thread for each chunk
		for(int i = 0; i < chunkFile1.size(); i++)
		{
			MapThread mrt = new MapThread(chunkFile1.get(i), 1);
			Thread t = new Thread(mrt);
			t.setName("chunk"+i);
			threadList.add(t);
			
			//run the thread
			threadList.get(i).start();
		}
		
		//join all threads
		for(Thread t : threadList)
		{
			try
			{
				t.join();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		
		/////////////////////////////////////////////////////////////////////////////
		/*
		* Reduce
		*/
		/////////////////////////////////////////////////////////////////////////////
		Reduce r = new Reduce("res\\task2kv1");
		
		fileOutput("///////////////////////////////////////////////\n", fw);
		fileOutput("Flight List: \n", fw);
		
		//print each flight
		for(KeyValuePair2 kv : r.getKV2())
		{
			//title for each flight is the flight id
			fileOutput("---- " + kv.getKey() + " ----\n", fw);
			
			//print each value
			for(Object f : kv.getValues())
			{
				//convert object into flight object
				String[] str = ((String) f).split(",");
				Flights flight = new Flights(str[0], str[1], str[2], str[3], str[4], str[5]);
				
				//print to the file
				fileOutput(flight.printFormat(), fw);
			}
			
			//System.out.println(kv);
		}
	}
	
	/**
	 * calculatePassengers
	 * calculates the number of passengers for each flight
	 * @throws IOException 
	 */
	public static void calculatePassengers() throws IOException
	{
		/////////////////////////////////////////////////////////////////////////////
		/*
		* Map
		*/
		/////////////////////////////////////////////////////////////////////////////
		
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		
		//make a new thread for each chunk
		for(int i = 0; i < chunkFile1.size(); i++)
		{
			MapThread mrt = new MapThread(chunkFile1.get(i), 2);
			Thread t = new Thread(mrt);
			t.setName("chunk"+i);
			threadList.add(t);
			
			//run the thread
			threadList.get(i).start();
		}
		
		//join all threads
		for(Thread t : threadList)
		{
			try
			{
				t.join();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		/////////////////////////////////////////////////////////////////////////////
		/*
		* Reduce
		*/
		/////////////////////////////////////////////////////////////////////////////
		Reduce r = new Reduce("res\\task2kv1");
		
		fileOutput("///////////////////////////////////////////////\n", fw);
		fileOutput("Passenger Count: \n", fw);
		
		//print each flight
		for(KeyValuePair2 kv : r.getKV2())
		{
			//title for each flight is the flight id
			fileOutput(kv.getKey() + ": " + kv.getValues().size() + "\n", fw);
		}
		
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
	 * VALIDATE_AIRPORT_NAME = 4
	 * VALIDATE_COORDINATES = 5
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
				
			//validate airport name
			case 4:
				//between 3 and 20 and has whitespace
				if(str.length() >= 3 && str.length() <= 20)
				{
					valid = true;
				}
				break;
				
			//validate coordinates
			case 5:
				if(str.matches("[+-]?\\d+{3}\\.?\\d+{6}\\s*"))
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
	 * @throws IOException 
	 */
	private static void fileOutput(String out, FileWriter fw) throws IOException
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
	
	/**
	 * emptyDirectory
	 * used to empty the directory which is passed to the function
	 * 
	 * @param f
	 */
	private static void emptyDirectory(File f)
	{
		//check if the path exists
		if(f.exists())
		{
			//get all its children and delete
			File[] contents = f.listFiles();
			for(File delete : contents)
			{
				//if it is a directory, delete its children
				if(delete.isDirectory())
				{
					emptyDirectory(delete);
				}
				else
				{
					if(!delete.delete())
					{
						System.out.println("Cannot delete: " + delete);
					}
				}
			}
		}
		else
		{
			System.out.println("File path does not exist, cannot empty contents");
		}
	}
}



















