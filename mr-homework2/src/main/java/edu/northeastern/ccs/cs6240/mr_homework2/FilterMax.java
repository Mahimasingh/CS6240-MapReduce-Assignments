package edu.northeastern.ccs.cs6240.mr_homework2;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import cs6240.hadoop_twitter_follower.CountFollower;

/**
 * Hello world!
 *
 */
public class FilterMax extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CountFollower.class);
	
	/*
	 * Tokenizer mapper tokenizes words in the input file and emits a <K,V>
	 */

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    word.set(user_follower[1]);
		    context.write(word, one);
		}
	}