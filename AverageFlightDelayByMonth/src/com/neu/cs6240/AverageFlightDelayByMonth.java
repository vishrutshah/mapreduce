package com.neu.cs6240;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AverageFlightDelayByMonth {
	public static class AverageFlightDelayByMonthMapper extends
			Mapper<Object, Text, AirlineTextPair, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		/**
		 * Key : Byte offset from where the line is being read 
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			AirlineTextPair key = new AirlineTextPair();
			Text arrDelayMinutes = new Text();

			if (line.length > 0 && isValidEntry(line)) {
				// set key as AirlineTextPair
				key.setAirLineName(line[6]);
				key.setMonth(line[2]);

				// set value as arrDelayMinutes
				arrDelayMinutes.set(line[37]);

				context.write(key, arrDelayMinutes);
			}
		}

		/**
		 * Function determines the validity of the input record
		 * 
		 * @param record : line record representing flight  details
		 * @return true iff entry contain all required data fields
		 * 		 	false otherwise
		 */
		private boolean isValidEntry(String[] record) {

			if (record == null || record.length == 0) {
				return false;
			}

			// If any of required field is missing, we'll ignore the record
			if (record[0].isEmpty() || record[2].isEmpty() || record[5].isEmpty()
					|| record[6].isEmpty() || record[37].isEmpty()
					|| record[41].isEmpty()) {
				return false;
			}

			if(! record[0].equals("2008")){
				return false;
			}

			// Whether flight was cancelled
			if (record[41].equals("1")) {
				return false;
			}

			return true;
		}
	}

	/**
	 * Reduce class
	 */
	public static class AverageFlightDelayByMonthReducer extends
			Reducer<AirlineTextPair, Text, Text, Text> {

		/**
		 * Reduce function will be called per Airline's Carrier Name and 
		 * months will be in increasing sorted order
		 * Key : AirlineTextPair which implements WritableComparable
		 * values : sorted delay in increasing order by month for given key airline 
		 */
		public void reduce(AirlineTextPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double totalDelay = 0;
			int totalFligths = 0;
			int currMonth = 1;
			StringBuilder output = new StringBuilder();
			output.append(key.getAirLineName().toString());
						
			for(Text value : values){
				// Check whether month has changed for the given flight
				// On change append data to output string and clear the counters
				if(currMonth != Integer.parseInt(key.getMonth().toString())){
					int avgDelay = (int)Math.ceil(totalDelay/(double)totalFligths);
					output.append(", (" + currMonth + "," + avgDelay + ")");
					totalDelay = 0;
					totalFligths = 0;
					currMonth = Integer.parseInt(key.getMonth().toString());
				}
				
				totalDelay += Double.parseDouble(value.toString());
				totalFligths++;
			}
			
			// Append last valid data of the current flight 
			int avgDelay = (int)Math.ceil(totalDelay/(double)totalFligths);
			output.append(", (" + currMonth + "," + avgDelay + ")");
			
			// Write down remaining months as 0 delay
			while(currMonth < 12)
			{
				currMonth++;
				output.append(", (" + currMonth + ", 0 )");
			}
			
			// Output the formatted string in the file 
			context.write(new Text(), new Text(output.toString()));
		}
	}
	
	/**
	 * AirlinePartitioner will partition data based on the hashcode of the 
	 * Airline's UniqueCarrier
	 */
	public static class AirlinePartitioner extends
			Partitioner<AirlineTextPair, Text> {
		/**
		 * Based on the configured number of reducer, this will
		 * partition the data approximately evenly based on number of 
		 * unique carrier names
		 */
		@Override
		public int getPartition(AirlineTextPair key, Text value,
				int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	/**
	 * KeyComparator first sorts based on the unique carrier name and 
	 * then sorts months in increasing order using compareTo method of the 
	 * AirlineTextPair class
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(AirlineTextPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AirlineTextPair airline1 = (AirlineTextPair) w1;
			AirlineTextPair airline2 = (AirlineTextPair) w2;
			return airline1.compareTo(airline2);
		}
	}

	/**
	 * GroupComparator ignores the month files of the key so that 
	 * single reduce function call receives all flight data for given 
	 * unique carrier name
	 */
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(AirlineTextPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AirlineTextPair airline1 = (AirlineTextPair) w1;
			AirlineTextPair airline2 = (AirlineTextPair) w2;
			return airline1.compare(airline2);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageFlighDelayByMonth <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Average monthly flight dealy");
		job.setJarByClass(AverageFlightDelayByMonth.class);
		job.setMapperClass(AverageFlightDelayByMonthMapper.class);
		job.setReducerClass(AverageFlightDelayByMonthReducer.class);
		job.setOutputKeyClass(AirlineTextPair.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(AirlinePartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
