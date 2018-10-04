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


@SuppressWarnings("deprecation")
public class RepJoin extends Configured implements Tool {
	
	private static final Logger logger = LogManager.getLogger(RepJoin.class);
	static int count = 0;
	
	
	public static class RepJoinMapper extends Mapper<Object, Text, IntWritable, Text> {
		private Map<String,List<String>> followerMap = new HashMap<String, List<String>>();
		
		public void setup(Context context) throws IOException,
		InterruptedException {
			try {
				Path[] files = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());

				if (files == null || files.length == 0) {
					throw new RuntimeException("User information is not set in DistributedCache");
		}

		// Read all files in the DistributedCache
		for (Path p : files) {
			BufferedReader rdr = new BufferedReader(
					new InputStreamReader(
							new FileInputStream(
									new File(p.toString()))));
			

			String line;
			// For each record in the user file
			while ((line = rdr.readLine()) != null) {
					String follower = line.split(",")[0];
					String followee = line.split(",")[1];
					if(followerMap.containsKey(follower)) {
						List<String> localFollowee = new ArrayList<>(followerMap.get(follower));
						localFollowee.add(followee);
						followerMap.put(follower, localFollowee);
					}
					else {
						List<String> localFollowee = new ArrayList<>();
						localFollowee.add(followee);
						followerMap.put(follower, localFollowee);
					}
				}
			rdr.close();
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
		    
		    if(followerMap.containsKey(user_follower[1])) {
		    	List<String> followees = new ArrayList<>(followerMap.get(user_follower[1]));
		    	for(int i = 0; i < followees.size(); i++) {
			    	String node3 = followees.get(i);
			    	List<String> node3_followees = followerMap.get(node3);
			    	if(node3_followees.contains(user_follower[0]))
			    		count = count + 1;
			    }
		    }
		    Text val = new Text();
		    IntWritable write = new IntWritable(count);
		    context.write(write, val);
		    
		    
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
		//DistributedCache.addCacheFile(new Path(args[0]).toUri(),
		//		job.getConfiguration());
		DistributedCache.addCacheFile(new URI("file:///Users/mahimasingh/mr/mr-git-folder/mahima/mr-homework2/input/input.csv"),
				job.getConfiguration());
		DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RepJoin(), args);
			System.out.println("Number of Triangles " + count);
			
		} catch (final Exception e) {
			logger.error("", e);
		}
	}


}
