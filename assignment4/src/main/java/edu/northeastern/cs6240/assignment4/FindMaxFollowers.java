package edu.northeastern.cs6240.assignment4;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
	
	/*static enum MaxValue {
        maximum
    }
    */
	static long maxValue;
	private static final Logger logger = LogManager.getLogger(FindMaxFollowers.class);
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    long followers = Long.parseLong(user_follower[1]);
		    /*Counter maxSoFar =
		            context.getCounter(FindMaxFollowers.MaxValue.maximum); */
		    if(followers > maxValue) {
		    	//maxSoFar.setValue(followers);
		    	maxValue = followers;
		    }
		    
		}
	}
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Maximum Follower value");
		job.setJarByClass(FindMaxFollowers.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		maxValue = Integer.MIN_VALUE;
		
		//max.setValue(Integer.MIN_VALUE);
		int code =  job.waitForCompletion(true)?0:1;
		/*Counters jobCounters = job.getCounters();
		Counter max = jobCounters.
        findCounter(MaxValue.maximum);
		long value = max.getValue(); */
		System.out.println("The maximum value of followers :"+ maxValue);
		return code;
		
	}
	public static void main(final String[] args) {
		try {
			ToolRunner.run(new FindMaxFollowers(), args);
		
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
	}


