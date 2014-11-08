package com.neu.cs6240;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountWithSiCombiner {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException { 
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  String inputWord = itr.nextToken();
    	  // Consider word if it's a valid word otherwise ignore it
    	  if(isValidWord(inputWord)){
    		  word.set(inputWord);
    		  context.write(word, one);
    	  }       
      }
    }
    /**
     * Check the give word is valid or not
     * @param word
     * @return true if word starts with M|m|N|n|O|o|P|p|Q|q otherwise false
     */
    public boolean isValidWord(String word){
    	return word.matches("^(M|m|N|n|O|o|P|p|Q|q).*$") ? true : false;
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /**
   * This class defines customer partition methods
   * @author vishrut
   */  
  public static class customerPartitioner extends Partitioner<Text, IntWritable>{
	/**
	 * Given a word as key, this function returns respective reduce task
	 * for this word
	 * Example: For key NEU this will return 0, i.e all the words
	 *          starting with N | n will for to reduce task 0
	 */ 
	@Override
	public int getPartition(Text key, IntWritable value, int numberOfReduceTask) {
		int partition = 0;
		String word = key.toString();
		
		if(word.startsWith("M") || word.startsWith("m")){
			partition = 0;
		}else if(word.startsWith("N") || word.startsWith("n")){
			partition = 1;
		}else if(word.startsWith("O") || word.startsWith("o")){
			partition = 2;
		}else if(word.startsWith("P") || word.startsWith("p")){
			partition = 3;
		}else if(word.startsWith("Q") || word.startsWith("q")){
			partition = 4;
		}else{
			partition = 0;
		}		
	
		return partition % numberOfReduceTask;
	}
	  
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count with si combiner");
    job.setJarByClass(WordCountWithSiCombiner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setPartitionerClass(customerPartitioner.class);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
