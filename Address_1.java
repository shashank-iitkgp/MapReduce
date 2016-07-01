package org.myorg;

import java.io.*;
//import java.util.*;
import au.com.bytecode.opencsv.CSVReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class Address_1 {
	public static class Address_Map extends Mapper <LongWritable, Text, Text,NullWritable> {
		
		private Text address = new Text();
	//	private int i = 0,j = 0;
		//private NullWritable x =new NullWritable();
		public void map (LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			if(key.get() > 0) {
				
				String check = context.getConfiguration().get("search_string").toLowerCase();
				String orig_val=value.toString();
				CSVReader r = new CSVReader(new StringReader(orig_val));
				String parsedline[] = r.readNext();
				r.close();
				String add_val = parsedline[1].concat(" " + parsedline[2]);
		/*		if(key.get() < 1000)
					System.out.println(add_val);
		/*	    String[] orig_val1=orig_val.split(",");
			    System.out.println("Iter "+i++ );
			    for(j=0;j<orig_val1.length;j++) {
			    	System.out.println(j+" "+ orig_val1[j]);
			    }*/
			    	
		//	    String add_val=orig_val1[2];
			    address.set(add_val);
			    if(add_val.toLowerCase().contains(check))
			    	context.write(address, NullWritable.get());
			     //String other_counts=orig_val1[2]+","+orig_val1[3]+","+orig_val1[4];
			     //context.write(), arg1)
			}			
		}
	}
	public static class Address_Reduce extends Reducer <Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key,NullWritable value,Context context) throws IOException,InterruptedException {
			context.write(key, value);
		}
	}
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("search_string", args[0]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Address_1");
		job.setJarByClass(Address_1.class);
		job.setMapperClass(Address_Map.class);
		job.setReducerClass(Address_Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.addFileToClassPath(new Path("/class/opencsv-2.4.jar"));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true)?0:1);		
	}
}
