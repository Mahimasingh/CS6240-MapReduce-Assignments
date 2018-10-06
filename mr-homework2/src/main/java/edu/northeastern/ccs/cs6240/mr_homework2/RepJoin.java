package edu.northeastern.ccs.cs6240.mr_homework2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/*
 * This class implements the Replicated partition + broadcast join in Map Reduce
 */

@SuppressWarnings("deprecation")
public class RepJoin extends Configured implements Tool {
	
	private static final Logger logger = LogManager.getLogger(RepJoin.class);
	
	
	
	public static class RepJoinMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
		// Hash map stores the data set in <Follower, <List of Followees>>
		private Map<String,List<String>> followerMap = new HashMap<String, List<String>>();
		static int count = 0;
		
		public void setup(Context context) throws IOException,
		InterruptedException {
			try {
				System.out.println("ENTERING.......");
				if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
					System.out.println("Found a cache File");
					URI mappingFileUri = context.getCacheFiles()[0];
					if (mappingFileUri != null) {
						BufferedReader rdr = new BufferedReader(
							new InputStreamReader(new FileInputStream(new File("./" + mappingFileUri + "/edges.csv"))));
						String line;
						while ((line = rdr.readLine()) != null) {
							String follower = line.split(",")[0];
							String followee = line.split(",")[1];
							if(Integer.parseInt(follower) < 10000 && Integer.parseInt(followee) <10000) {
								if(followerMap.containsKey(follower)) {
									//System.out.println("Map containes key. Size is" + followerMap.size());
									List<String> localFollowee = new ArrayList<>(followerMap.get(follower));
									localFollowee.add(followee);
									followerMap.put(follower, localFollowee);
								}
								else {
									//System.out.println("Map doesn't containes key. Size is" + followerMap.size());
									List<String> localFollowee = new ArrayList<>();
									localFollowee.add(followee);
									followerMap.put(follower, localFollowee);
								}
							}
						}
						rdr.close();
					}
				}
			}

	 catch (IOException e) {
		throw new RuntimeException(e);
	}
	

}

		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String[] user_follower = line.split(",");
		    
		    if(Integer.parseInt(user_follower[0]) < 1000 && Integer.parseInt(user_follower[1]) <1000) {
		    	// Here we first filter out the values which are below threshold and then using hash map,
		    	// we find the number of triangles in the data set
		    	if(followerMap.containsKey(user_follower[1])) {
		    		List<String> followees = new ArrayList<>(followerMap.get(user_follower[1]));
		    		for(int i = 0; i < followees.size(); i++) {
		    			String node3 = followees.get(i);
		    			List<String> node3_followees = new ArrayList<>(followerMap.get(node3));
		    			if(node3_followees.contains(user_follower[0])) {
		    				count = count + 1;
		    				System.out.println("Incremented count" + count);
		    			}
		    		}
		    		
		    	NullWritable nw = NullWritable.get();
			    IntWritable write = new IntWritable(count);
			    System.out.println("NUMBER OF TRIANGLES"+count/3);
			    context.write(write, nw);
			    
		    	}
		    }
		 }
	}

	@Override
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
	    final Job job = Job.getInstance(conf, "Count Followers");
		job.setJarByClass(RepJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(RepJoinMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.addCacheFile(new Path(args[0]).toUri());
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RepJoin(), args);
			
		} catch (final Exception e) {
			logger.error("", e);
		}
	}


}
