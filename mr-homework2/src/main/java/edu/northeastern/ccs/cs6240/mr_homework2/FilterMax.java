package edu.northeastern.ccs.cs6240.mr_homework2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
/*
 * @author : Mahima Singh
 * This class filters the values in Edges file which are above a threshhold MAX. 
 * Here program expects 3 parameter : input, output and max value
 */
public class FilterMax extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FilterMax.class);
	private static int maxValue = 0;
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {
		private final Text word = new Text();
		NullWritable nw = NullWritable.get();
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    if(Integer.parseInt(user_follower[0]) < FilterMax.maxValue && Integer.parseInt(user_follower[1]) < FilterMax.maxValue) {
		    	word.set(user_follower[0]+","+user_follower[1]);
		    	context.write(word,nw);
		    }
		    	
		    	
		}
	}

	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Count Followers");
		job.setJarByClass(FilterMax.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FilterMax.maxValue = Integer.parseInt(args[2]);
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new FilterMax(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}