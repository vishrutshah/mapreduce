package com.neu.cs6240;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AverageFlightDelay {
	/**
	 * Mapper class to apply projection on the flight data
	 */
	private static final String ORIGINATED_ORD = "ORIGINATED-ORD";
	private static final String ARRIVED_JFK = "ARRIVED-JFK";
	
	/**
	 * Global counter shared by all reduce tasks
	 */
	public enum HadoopCounter{
		TotalDelay,
		TotalFlights
	}
	
	public static class AverageFlighDelayMapper extends
			Mapper<Object, Text, Text, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',','"');
		
		/**
		 * Key : Byte offset from where the line is being read
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			Text key = new Text();
			Text record = new Text();
			
			if (line.length > 0 && isValidEntry(line)) {				
				// Set only record as (Flag, DepTime, ArrTime, ArrDelayMinutes)
				String mapRecord = getMapOutputRecord(line);
				record.set(mapRecord);
				
				// Set (FlightDate,IntermediateAirPort) as key
				if(mapRecord.contains(AverageFlightDelay.ORIGINATED_ORD)){
					key.set((line[5] + ":" + line[17]).toLowerCase());
				}else{
					key.set((line[5] + ":" + line[11]).toLowerCase());
				}
				
				context.write(key, record);
			}
		}
		
		/**
		 * Function determines the validity of the input record
		 * @param data
		 * @return
		 */
		private boolean isValidEntry(String[] record){
			
			if(record == null || record.length == 0){
				return false;
			}
			
			// If any of required field is missing, we'll ignore the record
			if(record[0].isEmpty() || record[2].isEmpty() || record[5].isEmpty() ||
					record[11].isEmpty() || record[17].isEmpty() || 
					record[24].isEmpty() || record[35].isEmpty() || 
					record[37].isEmpty() || record[41].isEmpty() ||
					record[43].isEmpty()){
				return false;
			}
			
			// whether flight belongs to June 2007 to May 2008 range
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");			
			Date flightDate = null;
			try {
				flightDate = dateFormatter.parse(record[5]);
				if(flightDate.before(dateFormatter.parse("2007-05-31")) || 
						flightDate.after(dateFormatter.parse("2008-06-01"))){
					return false;
				}
//				if(flightDate.before(dateFormatter.parse("2007-12-01")) || 
//						flightDate.after(dateFormatter.parse("2008-01-31"))){
//					return false;
//				}
			} catch (ParseException e) {
				// Unable to parse input date data
				return false;
			}
			
			// Whether flight was cancelled or diverted
			if(record[41].equals("1") || record[43].equals("1")){
				return false;
			}	
			
			// flight should not be originated from ORD and arrived at JFK
			// This will be considered as one legged flight
			if (record[11].toLowerCase().equals("ord") && record[17]
					.toLowerCase().equals("jfk")) {
				return false;
			}
			
			// whether flight was originated from ORD or arrived at JFK
			if(! (record[11].toLowerCase().equals("ord") 
					|| record[17].toLowerCase().equals("jfk"))){
				return false;
			}			
			return true;
		}
		
		/**
		 * Function generates the output record string for Map output
		 * @param record : array of string from input record
		 * @return : String comma separated line
		 */
		private String getMapOutputRecord(String[] record){
			StringBuilder output = new StringBuilder();
			
			if(record[11].toLowerCase().equals("ord")){
				// Set flag to determine flight originated from ORD
				output.append(AverageFlightDelay.ORIGINATED_ORD).append(",");	
			}else{
				// Set flag to determine flight arrived from JFK
				output.append(AverageFlightDelay.ARRIVED_JFK).append(",");
			}
			
			output.append(record[24]).append(",");	// DepTime
			output.append(record[35]).append(",");	// ArrTime
			output.append(record[37]);				// ArrDelayMinutes
			
			return output.toString();
		}
	}

	/**
	 * Reduce class to apply equi-join on the flight Mapper output data
	 */
	public static class AverageFlightDelayReducer extends
			Reducer<Text, Text, Text, Text> {
		
		private CSVParser csvParser = null;
		private int totalFlights;
		private float totalDelay;
		/**
	     * setup will be called once per Map Task before any of Map function call, 
	     * we'll initialize hashMap here
	     */
	    protected void setup(Context context){
	    	this.csvParser = new CSVParser(',','"');
	    	this.totalDelay = 0;
	    	this.totalFlights = 0; 
	    }
		
	    /**
	     * Reduce call will be made for every unique key value along with the 
	     * list of related records
	     */
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {	
			
			List<String> originatedAtORDList = new ArrayList<String>();
			List<String> arrivedAtJFKList = new ArrayList<String>();
			
			for(Text value : values){
				String record = value.toString();
				if(record.contains(AverageFlightDelay.ORIGINATED_ORD)){
					originatedAtORDList.add(record);
				}else{
					arrivedAtJFKList.add(record);
				}
			}
			
			for(String originatedAtORD : originatedAtORDList){
				for(String arrivedAtJFK : arrivedAtJFKList){
					String[] originatedAtORDData = this.csvParser.parseLine(originatedAtORD);
					String[] arrivedAtJFKData = this.csvParser.parseLine(arrivedAtJFK);
					
					// Here all flights are of same date and same intermediate 
					// airport
					
					if(isTwoLeggedFligh(originatedAtORDData, arrivedAtJFKData)){
						float delay = Float.parseFloat(originatedAtORDData[3]) +
								Float.parseFloat(arrivedAtJFKData[3]);						
						this.totalDelay += delay;
						this.totalFlights++;
					}
				}	
			}
		}
		
		/**
		 * Whether given two flight data is valid entry for two legged flight 
		 * starting at ORD and arriving at JFK? 
		 * @param originatedAtORDData array of string data of flight originated 
		 * 							  at ORD
		 * @param arrivedAtJFKData array of string data of flight arriving at JFK
		 * @return true iff this connection if valid two legged flight with valid time
		 * 		   false otherwise
		 */
		private boolean isTwoLeggedFligh(String[] originatedAtORDData,
				String[] arrivedAtJFKData) {

			// Whether flight reached to JFK was originated from where 
			// ORD originated flight landed?
			String ordArrivalTime = originatedAtORDData[2];
			String jfkDepartureTime = arrivedAtJFKData[1];
			if(Integer.parseInt(ordArrivalTime) < Integer.parseInt(jfkDepartureTime)){
				return true;
			}
			 return false;
		}
		
		/**
	     * cleanup will be called once per Map Task after all the Map function calls,
	     * we'll write hash of words to the file here
	     */
	    protected void cleanup(Context context) throws IOException, InterruptedException{
	    	context.getCounter(HadoopCounter.TotalDelay).increment((long)totalDelay);
			context.getCounter(HadoopCounter.TotalFlights).increment(totalFlights);
			context.write(new Text("--"+context.getCounter(HadoopCounter.TotalDelay).getValue()), 
					new Text("--"+context.getCounter(HadoopCounter.TotalFlights).getValue()));
	    }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: AverageFlighDelay <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
	    job.setJarByClass(AverageFlightDelay.class);
	    job.setMapperClass(AverageFlighDelayMapper.class);
	    job.setReducerClass(AverageFlightDelayReducer.class);
	    job.setNumReduceTasks(10);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
