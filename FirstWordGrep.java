package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class FirstWordGrep {

	public static class GrepMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		public  Path filesplit;
		private Text files = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            filesplit = ((FileSplit)context.getInputSplit()).getPath();
            String filename = filesplit.getName();
            files.set(filename);
  //          IntWritable one= new IntWritable(1);
			String line = value.toString().toLowerCase();
			StringTokenizer txt = new StringTokenizer(line); 
			String mapRegex = context.getConfiguration().get("mapregex").toLowerCase();
			if(key.get() == 0)
			{
				System.out.println("First line of " + filename);
				if(txt.nextToken().equalsIgnoreCase(mapRegex))
					context.write(files, NullWritable.get());
				
			}
		//	System.out.println(mapRegex);
//            System.out.println(filesplit.getName());
            /*
			while(txt.hasMoreTokens()) {
            	if (txt.nextToken().equals(mapRegex)) {
			//	System.out.println("Matched a line");
            		context.write(files, one);
            	}
            }*/
		}
	}/*
	public static class GrepReduce extends Reducer <Text, IntWritable, Text, NullWritable> {
		
		public void reduce(Text file, Iterable<IntWritable> values, Context context ) throws IOException,InterruptedException {
			Iterator <IntWritable> val = values.iterator();
			int sum =0;
			while(val.hasNext())			
				sum+=val.next().get();
			context.write(file, new IntWritable(sum));
			
			
		}
		
	}*/

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		conf.set("mapregex", args[0]);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "First Word Grep");
		job.setJarByClass(FirstWordGrep.class);
		job.setMapperClass(GrepMapper.class);
		//job.setReducerClass(GrepReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0); // Set number of reducers to zero
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}