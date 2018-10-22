package edu.northeastern.cs6240.hw3_mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
public class LongestPath extends Configured implements Tool {
	
	private static final Logger logger = LogManager.getLogger(LongestPath.class);
	
	public static class LongestPathMapper extends Mapper<Object, Text, LongWritable, Text>{
	private Logger logger = Logger.getLogger(this.getClass());
	@Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
		
		LongWritable outKey;
        Text outValue = new Text();
		
		String line = value.toString();
        String[] nodes = line.split("\\s+");
        
        long distance = Long.parseLong(nodes[2]);
        
        if(distance != Long.MAX_VALUE) {
        	outKey = new LongWritable(distance) ;
        	outValue.set(nodes[0]);
        	context.write(outKey, outValue);
        }
        
		}
	}
	
	public static class LongestPathReducer
    extends Reducer<LongWritable, Text, Text, LongWritable> {
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        
            for(Text t : values){
                context.write(t, key);
            }
            
        }
	}
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Two Paths");
		job.setJarByClass(LongestPath.class);
		final Configuration jobConf = job.getConfiguration();
		job.setMapperClass(LongestPathMapper.class);
		job.setReducerClass(LongestPathReducer.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(LongWritable.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
		
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new LongestPath(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}


