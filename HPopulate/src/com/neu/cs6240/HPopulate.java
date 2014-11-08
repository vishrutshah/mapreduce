package com.neu.cs6240;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;
/**
 * This class reads the csv file and populate the HBase table
 */
public class HPopulate {
	public static final String HBASE_TABLE_NAME = "FlightData";
	public static final String COLUMN_FAMILY = "Flight";
	public static final String COLUMN_YEAR = "year"; 
	public static final String COLUMN_CANCELLED = "cancelled"; 
	public static final String COLUMN_AVERAGE_DELAY_MIN = "delay"; 
	
	/**
	 * Mapper to connect to HBase and populate with data
	 */
	public static class HPopulateMapper extends
			Mapper<Object, Text, ImmutableBytesWritable, Writable> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = null;		
		private HTable table = null;
		
		/**
		 * setup will be called once per Map Task before any of Map function
		 * call, we'll initialize csvParser and HBase table connection here
		 * 
		 * @throws IOException
		 */
		protected void setup(Context context) throws IOException {
			this.csvParser = new CSVParser(',', '"');	
			
			org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
			table = new HTable(config, HBASE_TABLE_NAME);
			table.setAutoFlush(false);
			table.setWriteBufferSize(102400); // 100 MB buffer flush size
			
		}

		/**
		 * Key : Byte offset from where the line is being read 
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			StringBuilder rowKey = new StringBuilder();
			
			if (line.length > 0) {
				// set row key as UniqueCarrier#FlightDate#FlightNum#Origin
				// selected UniqueCarrier#FlightDate prefix entry to take advantage of 
				// HBase sorting and appended FlightNum#Origin to create unique rowKey
				rowKey.append(line[6]).append("#").append(line[5]).append("#")
					.append(line[10]).append("#").append(line[11]);
				
				float delay = line[37].isEmpty() ? 0f : Float.parseFloat(line[37]);
				String cancelled = (int)Float.parseFloat(line[41]) == 1 ? "1" : "0";
				
				Put record = new Put(Bytes.toBytes(rowKey.toString()));
				record.add(COLUMN_FAMILY.getBytes(), COLUMN_CANCELLED.getBytes(), cancelled.getBytes());
				record.add(COLUMN_FAMILY.getBytes(), COLUMN_AVERAGE_DELAY_MIN.getBytes(), String.valueOf(delay).getBytes());
				record.add(COLUMN_FAMILY.getBytes(), COLUMN_YEAR.getBytes(), line[0].getBytes());
				
				table.put(record);
			}
		}
		
	    /**
	     * cleanup will be called once per Map Task after all the Map function calls,
	     * we'll close Hbase table connection here
	     */
	    protected void cleanup(Context context) throws IOException, InterruptedException{    	
	    	table.close();
	    }		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: H-POPULATE <in> <out>");
			System.exit(2);
		}
		
		// Create table for HBase 
		createHbaseTable();
		
		// Configure job
		Job job = new Job(conf, "Create HBase table and fill it up.");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);		
		job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
	    // set partitioner to 0 explicitly as not required
	    job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * Connects to the HBase instance and creates table. If table already exists then
	 * it deletes the existing table and creates again
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 */
	public static void createHbaseTable() throws IOException, ZooKeeperConnectionException
	{
		HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
		HTableDescriptor ht = new HTableDescriptor(HBASE_TABLE_NAME);
		ht.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
		HBaseAdmin hba = new HBaseAdmin(hc);
		if(hba.tableExists(HBASE_TABLE_NAME))
		{
			hba.disableTable(HBASE_TABLE_NAME);
			hba.deleteTable(HBASE_TABLE_NAME);				
		}
		hba.createTable(ht);
		hba.close();
	}
}
