package edu.northeastern.cs6240.assignment4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import edu.northeastern.cs6240.assignment4.KMeans.KMapper.KReducer;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

@SuppressWarnings("deprecation")
public class KMeans extends Configured implements Tool {
	static enum SumOfSquaredError {
        sumError
    }
	private static final Logger logger = LogManager.getLogger(KMeans.class);
	
	public static String SPLITTER = "\t| ";
	
	
	public static class KMapper extends Mapper<Object, Text, DoubleWritable, Text> {
		
		public List<Double> centers = new ArrayList<Double>();
		@Override
	    public void setup(Context context) throws IllegalArgumentException, IOException {
	       
			try {
				// Fetch the file from Distributed Cache Read it and store the
				// centroid in the ArrayList
				URI[] cacheFiles = context.getCacheFiles();
				for(int i=0; i<cacheFiles.length; i++) {
					String line;
					URI cacheFile = cacheFiles[i];
					FileSystem fs = FileSystem.get(cacheFile, new Configuration());
					InputStreamReader fis = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
					BufferedReader cacheReader = new BufferedReader(fis);
					try {
						// Read the file split by the splitter and store it in
						// the list
						while ((line = cacheReader.readLine()) != null) {
							centers.add(Double.parseDouble(line));
							System.out.println(centers);
						}
					} finally {
						cacheReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistribtuedCache: " + e);
			}
		}

		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
	    String[] user_follower = line.split("\\s+");
	    
	    double followers = Double.parseDouble(user_follower[1]);
	    double closestCenter = Integer.MAX_VALUE;
	    double distanceFromCenter = Math.abs(closestCenter - followers); 
	    
	    for(double center : centers) {
	    	double tempDistance = Math.abs(center - followers);
	    	if(tempDistance < distanceFromCenter) {
	    		closestCenter = center;
	    		distanceFromCenter = tempDistance;
	    	}
	    }
	    DoubleWritable outKey = new DoubleWritable(closestCenter);
	    context.write(outKey, value);
	}
		
	public static class KReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double newCenter;
			double sum = 0;
			int no_elements = 0;
			List<Double> elements = new ArrayList<Double>();
			for(Text val : values) {
				double followerValue = Double.parseDouble(val.toString().split("\\s+")[1]);
				sum = sum + followerValue;
				elements.add(followerValue);
				no_elements++;
			}
			newCenter = sum/no_elements;
			
			calculateSumSquaredError(key.get(),elements,context);
			Text outVal = new Text();
			DoubleWritable outKey = new DoubleWritable(newCenter);
			context.write(outKey,null);
		}
		
		private void calculateSumSquaredError(double center, List<Double> values , Context context) {
			
			double sumSquaredError = 0;
			for(double val : values) {
				sumSquaredError = sumSquaredError + Math.pow(Math.abs(val - center), 2);
			}
			Counter error =
		            context.getCounter(KMeans.SumOfSquaredError.sumError);
			sumSquaredError = sumSquaredError * 100;
			error.increment(Math.round(sumSquaredError));
		}
	}
	
	}
	public int run(String[] args) throws Exception {
		int iteration = 0;
		boolean isdone = false;
		String input, output;
		int code = 0;
		while (isdone == false && iteration < 11) {
			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, "K-Means");
			
			System.out.println("----------------------------ITERATION" + iteration + "-------------------------------");
			input = args[0];
			if(iteration == 0) {
				output = args[1]+"/"+iteration;
				addCentroidFileToCache(args[2],conf,job);
			}
			else {
				output = args[1] + "/" + iteration;
				String centroidFileName = args[1]+"/"+(iteration-1);
				addCentroidFileToCache(centroidFileName,conf,job);
			}
			
			FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMapper.class);
            job.setReducerClass(KReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            /*if(iteration > 1) {
            	List<Double> newCenters = getNextCenterPoints(args[1] +"/" + (iteration-1)+"/part-r-00000");
            	List<Double> prevCenters = getPreviousCenterPoints(args[1] + "/" + (iteration -2)+"/part-r-00000");
            	Collections.sort(newCenters);
            	Collections.sort(prevCenters);
            	Iterator<Double> it = prevCenters.iterator();
            	for (double d : newCenters) {
            		double temp = it.next();
            		if (Math.abs(temp - d) <= 0.1) {
            			isdone = true;
            		} else {
            			isdone = false;
            			break;
            		}
            }} */
            ++iteration;
			
			code = job.waitForCompletion(true) ? 0 : 1;
			Counters jobCounters = job.getCounters();
            long errorVal = jobCounters.
                findCounter(SumOfSquaredError.sumError).getValue();
            System.out.println("\n \n The value of Sum Squared Error " + errorVal/100.0 + "\n\n");
			
		}
		return code;
	}
	
	public void addCentroidFileToCache(String folder,Configuration conf,Job job) throws Exception {
		System.out.println("Adding files to cache");
		Path hdfsPath = new Path(folder);
		FileSystem fs = FileSystem.get(new URI(folder), new Configuration());
		FileStatus[] fStatuses = fs.listStatus(hdfsPath);
		for(FileStatus f : fStatuses) {
			job.addCacheFile(f.getPath().toUri());
		}
		
	}
	
	public List<Double> getPreviousCenterPoints(String folder) throws IOException{
		Path prevfile = new Path(folder);
		FileSystem fs1 = FileSystem.get(new Configuration());
		BufferedReader br1 = new BufferedReader(new InputStreamReader(
				fs1.open(prevfile)));
		List<Double> centers_prev = new ArrayList<Double>();
		String l = br1.readLine();
		while (l != null) {
			String[] sp1 = l.split(SPLITTER);
			double d = Double.parseDouble(sp1[0]);
			centers_prev.add(d);
			l = br1.readLine();
		}
		br1.close();
		return centers_prev;
	}
	
	public List<Double> getNextCenterPoints(String folder) throws IOException {
		
		Path currentFile = new Path(folder);
		FileSystem fs1 = FileSystem.get(new Configuration());
		BufferedReader br1 = new BufferedReader(new InputStreamReader(
				fs1.open(currentFile)));
		List<Double> centers_next = new ArrayList<Double>();
		String l = br1.readLine();
		while (l != null) {
			String[] sp1 = l.split(SPLITTER);
			double d = Double.parseDouble(sp1[0]);
			centers_next.add(d);
			l = br1.readLine();
		}
		br1.close();
		return centers_next;
	}
	
	
	public static void main(String args[]) {
		
		try {
			ToolRunner.run(new KMeans(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
