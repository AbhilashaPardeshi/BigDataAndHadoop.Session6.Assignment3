package Assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Step1.TelevisionSalesC;

/**
 * 
 * @author abhilasha
 *
 */
public class TelevisionDriver 
{
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		//Check if input parameters provided appropriately
		if(args==null || args.length!=2)
		{
			System.err.println("Incorrect input parameters provided");
			System.exit(-1);
		}
	
		
		//Instantiate configuration object
		Configuration conf = new Configuration();
		
		//Instantiate job object
		Job job  = new Job(conf,"Television");
		
	
		job.setJarByClass(TelevisionDriver.class);
	
		/*
		 * Set input pathput
		 * abhilasha/television.txt
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		/*
		 * Set output path
		 * eg : /abhilasha/TelevisionSalesOutput
		 */
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Delete output directory if already existing
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		//Set mapper class
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		job.setPartitionerClass(TelevisionPartitioner.class);
			
		//Number of reducers
		job.setNumReduceTasks(5);
		
		
		//Sorting classes
		job.setGroupingComparatorClass(TelevisionGroupingComparator.class);
		
		
		//Set input and output format class
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set mapper key-value class types
		job.setMapOutputKeyClass(Television.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		
		//Set output key'value class types
		job.setOutputKeyClass(Television.class);
		job.setOutputValueClass(LongWritable.class);
				
		//Execute the job and wait until completion and then exit
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
