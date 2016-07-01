package org.myorg;

import java.io.*;
//import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class SubString {

	public static class GrepMapper extends
			Mapper<Object, Text, Text, Text> {
		public  Path filesplit;
		private Text files = new Text();
		private Text val = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
            filesplit = ((FileSplit)context.getInputSplit()).getPath();
            String filename = filesplit.getName(); // String a;
    //       files.set(filename);
    //        IntWritable one= new IntWritable(1);
            val.set(value.toString());
			String line = value.toString().toLowerCase();
		//	StringTokenizer txt = new StringTokenizer(line); 
			String mapRegex = context.getConfiguration().get("mapregex").toLowerCase();
			if(line.contains(mapRegex.toLowerCase())){
				
		
				files.set(filename);
				context.write(files, val);
			
			}
			System.out.println(line.contains(mapRegex.toLowerCase()));
		//	System.out.println(mapRegex);
//            System.out.println(filesplit.getName());
/*            while(txt.hasMoreTokens()) {
            	if (txt.nextToken().equals(mapRegex)) {
			//	System.out.println("Matched a line");
            		context.write(files, one);
            	}
            }*/
		}
	}
	public static class GrepReduce extends Reducer <Text, Text, Text, Text> {
		
		public void reduce(Text value, Text file, Context context ) throws IOException,InterruptedException {
			context.write(file, value);
			
			
		}
		
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		conf.set("mapregex", args[0]);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Distributed Grep");
		job.setJarByClass(SubString.class);
		job.setMapperClass(GrepMapper.class);
		job.setReducerClass(GrepReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	//	job.setNumReduceTasks(0); // Set number of reducers to zero
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}