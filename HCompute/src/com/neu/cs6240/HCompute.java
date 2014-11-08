package com.neu.cs6240;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This program fetches data from HBase and computes the
 * average flight delay by month
 */
public class HCompute {
	public static final String HBASE_TABLE_NAME = "FlightData";
	public static final String COLUMN_FAMILY = "Flight";
	public static final String COLUMN_YEAR = "year";
	public static final String COLUMN_CANCELLED = "cancelled";
	public static final String COLUMN_AVERAGE_DELAY_MIN = "delay";

	public static class HComputeMapper extends
			TableMapper<AirlineTextPair, Text> {
		/**
		 * row : Row key from the HBase Table
		 * value: REntire row value from the table respective to row key
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			AirlineTextPair key = new AirlineTextPair();
			Text arrDelayMinutes = new Text();

			// Parse the row value from the HBase			
			String[] hbaseRowKey = new String(value.getRow()).split("#");

			key.setAirLineName(hbaseRowKey[0]);
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
			Date flightDate = null;
			try {
				flightDate = dateFormatter.parse(hbaseRowKey[1]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Invalid date detected");
			}
			key.setMonth("" + (flightDate.getMonth() + 1));
			// set value as arrDelayMinutes
			arrDelayMinutes.set(new String(value.getValue(
					COLUMN_FAMILY.getBytes(),
					COLUMN_AVERAGE_DELAY_MIN.getBytes())));
			
			// output record as (UniqueCarrier, Month), ArrDelayMinutes 
			context.write(key, arrDelayMinutes);
		}
	}

	public static class HComputeReducer extends
			Reducer<AirlineTextPair, Text, Text, Text> {
		/**
		 * reducer will get all the records sorted by increasing month for a 
		 * airline
		 */
		public void reduce(AirlineTextPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			float totalDelay = 0;
			int totalFligths = 0;
			int currMonth = 1;
			StringBuilder output = new StringBuilder();
			output.append(key.getAirLineName().toString());

			for (Text value : values) {
				// Check whether month has changed for the given flight
				// On change append data to output string and clear the counters				
				if (currMonth != Integer.parseInt(key.getMonth().toString())) {
					int avgDelay = (int) Math.ceil(totalDelay
							/ (float) totalFligths);					
					output.append(", (" + currMonth + "," + avgDelay + ")");
					totalDelay = 0;
					totalFligths = 0;
					currMonth = Integer.parseInt(key.getMonth().toString());
				}
				 
				totalDelay += Float.parseFloat(value.toString());
				totalFligths++;
			}
			
			// Append last valid data of the current flight
			int avgDelay = (int) Math.ceil(totalDelay / (double) totalFligths);
			output.append(", (" + currMonth + "," + avgDelay + ")");

			// Write down remaining months as 0 delay
			while (currMonth < 12) {
				currMonth++;
				output.append(", (" + currMonth + ", 0)");
			}

			// Output the formatted string in the file 			
			context.write(new Text(), new Text(output.toString()));
		}
	}

	/**
	 * HComputePartitioner will partition data based on the hashcode of the 
	 * Airline's UniqueCarrier
	 */	
	public static class HComputePartitioner extends
			Partitioner<AirlineTextPair, Text> {

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
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * 
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: H-COMPUTE <out>");
			System.exit(2);
		}

		// Apply filter while retrieving data from HBase
		FilterList filterList = new FilterList(
				FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter yearFilter = new SingleColumnValueFilter(
				COLUMN_FAMILY.getBytes(), COLUMN_YEAR.getBytes(),
				CompareOp.EQUAL, "2008".getBytes());
		filterList.addFilter(yearFilter);
		SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter(
				COLUMN_FAMILY.getBytes(), COLUMN_CANCELLED.getBytes(),
				CompareOp.EQUAL, "0".getBytes());
		filterList.addFilter(cancelledFilter);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setFilter(filterList);

		Job job = new Job(conf, "H-Compute");
		job.setJarByClass(HCompute.class);
		job.setMapperClass(HComputeMapper.class);
		job.setReducerClass(HComputeReducer.class);
		job.setOutputKeyClass(AirlineTextPair.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(HComputePartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	
		TableMapReduceUtil.initTableMapperJob(HBASE_TABLE_NAME, scan,
				HComputeMapper.class, AirlineTextPair.class, Text.class, job);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
