package edu.northeastern.cs6240.assignment4;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

public class FindMaxFollowers extends Configured implements Tool {
	

	
	private static final Logger logger = LogManager.getLogger(FindMaxFollowers.class);
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		
		
		public static double maxValue = 0;
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split("\\s+");
		    double followers = Double.parseDouble(user_follower[1]);
		    if(followers > maxValue) {
		    	maxValue = followers;
		    	DoubleWritable outVal = new DoubleWritable();
			    outVal.set(maxValue);
			    Text outKey = new Text("global");
			    context.write(outKey, outVal);
		    }
		    
		    
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
			double maximum = 0;
			for (DoubleWritable val : values) {
				if(maximum < val.get())
					maximum = val.get();
			}
			Text outKey = new Text();
			DoubleWritable outVal = new DoubleWritable();
		    outVal.set(maximum);
			context.write(outKey, outVal);
		}
	}
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Maximum Follower value");
		job.setJarByClass(FindMaxFollowers.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(final String[] args) {
		try {
			ToolRunner.run(new FindMaxFollowers(), args);
		
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
	}


